#!/usr/bin/env python3

import os
import pip
import pandas as pd
import logging
from redcap import Project
from collections import defaultdict
import flywheel_gear_toolkit
from utils.utils import (
    get_sessions_pi_copy,
    get_sessions_redcap,
    get_hdr_fields,
    get_first_acq,
    find_matches,
    generate_wbhi_id,
    tag_session_redcap,
    tag_session_wbhi,
    mv_session,
    smarter_copy,
    split_session
)
from utils.flywheel import (
    run_gear
)

pip.main(["install", "--upgrade", "git+https://github.com/poldracklab/wbhi-utils.git"])
from wbhiutils.constants import ( # noqa: E402
    SITE_LIST,
    REDCAP_API_URL
)

log = logging.getLogger(__name__)

WAIT_TIMEOUT = 3600 * 2

def pi_copy(site: str) -> None:
    """Finds acquisitions in the site's 'Inbound Data' project that haven't
    been smart-copied yet. Determines the pi-id from the dicom and smart-copies
    to project named after pi-id."""
    log.info(f"Checking {site} acquisitions to smart-copy.")
    site_project = client.lookup(f"{site}/Inbound Data")
    sessions = get_sessions_pi_copy(site_project)
    copy_dict = defaultdict(list)
    
    for session in sessions:
        hdr_list = []
        for acq in session.acquisitions():
            acq_hdr_fields = get_hdr_fields(acq, site)
            if acq_hdr_fields["error"]:
                continue
            hdr_list.append(acq_hdr_fields)
            if acq_hdr_fields["pi_id"].isalnum():
                pi_id = acq_hdr_fields["pi_id"]
            else:
                pi_id = "other"
            if f"copied_{pi_id}" not in acq.tags:
                copy_dict[pi_id].append(acq)
 
        if hdr_list and 'skip_split' not in session.tags:
            split_session(session, hdr_list)
    
    if copy_dict:
        group = client.get_group(site)
        for pi_id, acq_list in copy_dict.items():
            pi_project = group.projects.find_first(f"label={pi_id}")
            if not pi_project:
                client.add_project(body={'group':site, 'label':pi_id})
                pi_project = client.lookup(os.path.join(site, pi_id))
            smarter_copy(acq_list, site_project, pi_project, client)
    else:
        log.info("No acquisitions were smart-copied.")
                
def redcap_match_mv(
    site: str,
    redcap_data: list,
    redcap_project: Project,
    id_list: list) -> None:
    """Find sessions that haven't been checked or that are scheduled to be checked today.
    Pulls relevant fields from dicom and checks for matches with redcap records. If matches,
    generate unique WBHI-ID and assign to flywheel subject and matching records (or pull from
    redcap if WBHI-ID already exists.) Finally, move matching subjects to wbhi/pre-deid project."""
    log.info(f"Checking {site} for matches with redcap.")
    new_records = []
    wbhi_id_session_dict = {}
    
    pre_deid_project = client.lookup('wbhi/pre-deid')
    site_project = client.lookup(f"{site}/Inbound Data")
    sessions = get_sessions_redcap(site_project)
    
    if not sessions:
        log.info(f"No sessions were checked for {site}/Inbound Data.")
        return
    for session in sessions:
        first_acq = get_first_acq(session)
        if not first_acq:
            continue
        hdr_fields = get_hdr_fields(first_acq, site)
        if hdr_fields["error"]:
            continue
        
        matches = find_matches(hdr_fields, redcap_data)
        if matches:
            wbhi_id = generate_wbhi_id(matches, site, id_list)
            wbhi_id_session_dict[wbhi_id] = session
            for match in matches:
                match["rid"] = wbhi_id
                new_records.append(match)
        else:
            tag_session_redcap(session)
        
    if new_records:
        # Import updated records into RedCap
        response = redcap_project.import_records(new_records)
        if response["count"] == len(new_records):
            for wbhi_id, session in wbhi_id_session_dict.items():
                tag_session_wbhi(session)
                subject = client.get_subject(session.parents.subject)
                subject.update({'label': wbhi_id})
                mv_session(session, pre_deid_project)
            log.info(
                f"Updated REDCap and Flywheel to include newly generated wbhi-id(s): "
                f"{wbhi_id_session_dict.keys()}"
            )
        else:
            log.error("Failed to update records on REDCap")
    else:
        log.info("No matches found on REDCap")

def manual_match(csv_path: str, redcap_data: list, redcap_project: Project, id_list: list) -> None:
    """Manually matches a flywheel session and a redcap record."""

    match_df = pd.read_csv(csv_path, names=('site', 'participant_id', 'sub_label'))
    match_df['sub_label'] = match_df['sub_label'].str.replace(',', '\,')
    pre_deid_project = client.lookup('wbhi/pre-deid')

    for i, row in match_df.iterrows():
        project = client.lookup(f'{row.site}/Inbound data')
        subject = project.subjects.find_first(f'label={row.sub_label}')
        if not subject:
            log.error(f"Flywheel subject {row.sub_label} was not found.")
            continue
        record = next(
            (item for item in redcap_data if item["participant_id"] == str(row.participant_id)),
            None
        )
        if not record:
            log.error(f"Redcap record {row.participant_id} was not found.")
            continue

        wbhi_id = generate_wbhi_id([record], row.site, id_list)
        record["rid"] = wbhi_id
        response = redcap_project.import_records([record])
        if 'error' in response:
            log.error(f"Redcap record {row.participant_id} failed to update.")
            continue
        subject.update({'label': wbhi_id})
        id_list.append(wbhi_id)
        sessions = subject.sessions()
        for session in sessions:
            tag_session_wbhi(session)
            mv_session(session, pre_deid_project)

        log.info(f"Updated REDCap and Flywheel to include newly generated wbhi-id: {wbhi_id}")
    

def deid() -> None:
    """Runs the deid-export gear for any acquisitions in wbhi/pre-deid for which
    it hasn't already been run. Since the gear doesn't wait to check if the 
    deid-export runs are successful, it checks if each acquisition already exists in
    the destination project (wbhi/deid) prior to running, and tags and ignores if 
    already exists."""
    pre_deid_project = client.lookup('wbhi/pre-deid')
    deid_project = client.lookup('wbhi/deid')
    deid_gear = client.lookup('gears/deid-export')
    deid_template = pre_deid_project.get_file('deid_profile.yaml')
    inputs = {'deid_profile': deid_template}
    config = {
        'project_path': 'wbhi/deid', 
        'overwrite_files': 'Skip',
        'debug': False,
    } 
    for session in pre_deid_project.sessions():
        if "deid" not in session.tags:
            # If already deid, tag and ignore
            sub_label = client.get_subject(session.parents.subject).label.replace(',', '\,')
            dst_subject = deid_project.subjects.find_first(f'label="{sub_label}"')
            if dst_subject:
                session_label = session.label.replace(',', '\,')
                dst_session = dst_subject.sessions.find_first(f'label="{session_label}"')
                if dst_session:
                    src_acq_set = set([acq.label for acq in session.acquisitions()])
                    dst_acq_set = set([acq.label for acq in dst_session.acquisitions()])
                    if src_acq_set == dst_acq_set:
                        session.add_tag('deid')
                        continue
            # Otherwise, run deid gear
            run_gear(deid_gear, inputs, config, session)

def main():
    gtk_context.init_logging()
    gtk_context.log_config()

    redcap_api_key = config["redcap_api_key"]
    redcap_project = Project(REDCAP_API_URL, redcap_api_key)
    redcap_data = redcap_project.export_records()
    id_list = [record["rid"] for record in redcap_data]

    match_csv = gtk_context.get_input_path("match_csv")
    if match_csv:
        manual_match(match_csv, redcap_data, redcap_project, id_list)
        deid()
    else:
        for site in SITE_LIST:
            pi_copy(site)
            redcap_match_mv(site, redcap_data, redcap_project, id_list)
            deid()
    
    log.info("Gear complete. Exiting.")

if __name__ == "__main__":
    with flywheel_gear_toolkit.GearToolkitContext() as gtk_context:
        config = gtk_context.config
        client = gtk_context.client
        
        main()

