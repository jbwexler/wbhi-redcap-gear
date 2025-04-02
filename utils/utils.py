#!/usr/bin/env python3

import random
import string
import pandas as pd
import logging
from datetime import datetime, timedelta
from flywheel import (
    ProjectOutput,
    SessionListOutput,    
    AcquisitionListOutput,
    SubjectOutput
)
from utils.flywheel import (
    smart_copy,
    delete_project,
    create_view_df,
    mv_session
)
from wbhiutils import parse_dicom_hdr
from wbhiutils.constants import (
    SITE_LIST,
    DATETIME_FORMAT_FW,
    DATE_FORMAT_FW,
    DATE_FORMAT_RC,
    SITE_KEY,
    REDCAP_KEY,
    WBHI_ID_SUFFIX_LENGTH
)


log = logging.getLogger(__name__)

def get_sessions_pi_copy(fw_project: ProjectOutput) -> list:
    """Get and filter sessions for pi_copy()"""
    sessions = []
    for session in fw_project.sessions():
        if any(tag.startswith('copied_') for tag in session.tags):
            continue
        sessions.append(session)
    return sessions

def get_sessions_redcap(fw_project: ProjectOutput) -> list:
    """Get and filter sessions for redcap_match_mv"""
    sessions = []
    today = datetime.today()
    now = datetime.utcnow()
    for session in fw_project.sessions():
        if "skip_redcap" in session.tags or "need_to_split" in session.tags:
            log.info(f'Skipping session {session.label} due to tag')
            continue
            
        # Remove timezone info from timestamp
        timestamp = session.timestamp.replace(tzinfo=None)
        if (now - timestamp) < timedelta(days=config["ignore_until_n_days_old"]):
            continue

        redcap_tags = [t for t in session.tags if t.startswith('redcap')]
        if not redcap_tags:
            sessions.append(session)
            continue
        elif len(redcap_tags) > 1:
            log.warning(f"{session.label} has multiple redcap tags: {redcap_tags}")
            redcap_tags = [sorted(redcap_tags)[-1]]
        tag_date_str = redcap_tags[0].split('_')[-1]
        tag_date = datetime.strptime(tag_date_str, DATE_FORMAT_FW)
        if tag_date <= today:
            sessions.append(session)
    return sessions
    
def get_acq_or_file_path(container, client) -> str:
    """Takes a container and returns its path."""
    project_label = client.get_project(container.parents.project).label
    sub_label = client.get_subject(container.parents.subject).label
    ses_label = client.get_session(container.parents.session).label

    if container.container_type == 'acq':
        return f"{project_label}/{sub_label}/{ses_label}/{container.label}"
    elif container.container_type == 'file':
        acq_label = client.get_acquisition(container.parents.acquisition).label
        return f"{project_label}/{sub_label}/{ses_label}/{acq_label}/{container.name}"

def get_hdr_fields(acq: AcquisitionListOutput, site: str) -> dict:
    """Get relevant fields from dicom header of an acquisition"""
    dicom_list = [f for f in acq.files if f.type == "dicom"]
    if not dicom_list:
        log.warning(f"{get_acq_or_file_path(acq, client)} contains no dicoms.")
        return {"error": "NO_DICOMS"}
    dicom = dicom_list[0].reload()

    if "file-classifier" not in dicom.tags or "header" not in dicom.info:
        log.error(f"File-classifier gear has not been run on {get_acq_or_file_path(acq, client)}")
        return {"error": "FILE_CLASSIFIER_NOT_RUN"}
    
    dcm_hdr = dicom.info["header"]["dicom"]

    try:
        return {
            "error": None,
            "acq": acq,
            "site": site,
            "pi_id": parse_dicom_hdr.parse_pi(dcm_hdr, site).casefold(),
            "sub_id": parse_dicom_hdr.parse_sub(dcm_hdr, site).casefold(),
            "date": datetime.strptime(dcm_hdr["StudyDate"], DATE_FORMAT_FW),
            "am_pm": "am" if float(dcm_hdr["StudyTime"]) < 120000 else "pm",
            "series_datetime": datetime.strptime(
                f"{dcm_hdr['SeriesDate']} {dcm_hdr['SeriesTime']}",
                DATETIME_FORMAT_FW
            )
        }
    except KeyError:
        log.warning(f"{get_acq_or_file_path(dicom, client)} is missing necessary field(s).")
        breakpoint()
        return {"error": "MISSING_DICOM_FIELDS"}

def split_session(session: SessionListOutput, hdr_list: list) -> None:
    """Checks to see if a sessions is actually a combination of multiple sessions.
    If so, logs an error and exits.
    
    To-do: actually implement splitting
    """
    hdr_df = pd.DataFrame(hdr_list)
    
    # Don't split if file classifier gear hasn't been run on all acquisitions
    if "FILE_CLASSIFIER_NOT_RUN" in hdr_df["error"].values:
        return
def get_first_acq(session: SessionListOutput) -> AcquisitionListOutput | None:
    """Gets first acquisition in session."""
    acq_list = session.acquisitions()
    acq_sorted = sorted(acq_list, key=lambda d: d.timestamp)
    if acq_sorted:
        return acq_sorted[0]

def find_matches(hdr_fields: dict, redcap_data: list) -> list | None:
    """Finds redcap records that match relevant header fields of a dicom."""
    matches = []
    # Start with most recent records
    for record in reversed(redcap_data):
        if (record["icf_consent"] == "1"
            and record["consent_complete"] == "2"
            and record["site"] == hdr_fields["site"]
            and record["site"] in SITE_LIST
            and datetime.strptime(record["mri_date"], DATE_FORMAT_RC) == hdr_fields["date"] 
            and REDCAP_KEY["am_pm"][record["mri_ampm"]] == hdr_fields["am_pm"]
            and record["mri"].casefold() == hdr_fields["sub_id"]):
            
            mri_pi_field = "mri_pi_" + hdr_fields["site"]
            if (record[mri_pi_field].casefold() == hdr_fields["pi_id"] 
                or (record[mri_pi_field] == '99'
                and record[f"{mri_pi_field}_other"].casefold() == hdr_fields["pi_id"])):

                matches.append(record)

    return matches

def generate_wbhi_id(matches: list, site: str, id_list: list) -> str:
    """Generates a unique WBHI-ID for a subject, or pulls it from redcap if a
    WBHI-ID already exists for this match (in the "rid" field). Also mutates
    id_list if a new WBHI-ID is generated."""
    wbhi_id_prefix = SITE_KEY[site]
    
    for match in matches:
        # Use pre-existing WBHI-ID from redcap record
        if match["rid"] and match["rid"].strip():
            wbhi_id = match["rid"]
            return wbhi_id
            
    # Generate ID and make sure it's unique
    while True:
        wbhi_id_suffix = ''.join(random.choices(
            string.ascii_uppercase + string.digits,
            k=WBHI_ID_SUFFIX_LENGTH
        ))
        wbhi_id = wbhi_id_prefix + wbhi_id_suffix
        if wbhi_id not in id_list:
            id_list.append(wbhi_id)
            return wbhi_id
            
def tag_session_wbhi(session: SessionListOutput) -> None:
    """Tags a session with 'wbhi' and removes any redcap tags"""
    redcap_tags = [tag for tag in session.tags if tag.startswith('redcap')]
    session.add_tag("wbhi")
    if redcap_tags:
        for tag in redcap_tags:
            session.delete_tag(tag)
    for acq in session.acquisitions():
        for f in acq.files:
            f.add_tag("wbhi")

def tag_session_redcap(session: SessionListOutput) -> None:
    """Tags with redcap tag containing the date for the next check by this gear."""
    redcap_tags = [tag for tag in session.tags if tag.startswith('redcap')]
    if redcap_tags:
        redcap_tag = sorted(redcap_tags)[-1]
        n = int(redcap_tag.split("_")[1])
        for tag in redcap_tags:
            session.delete_tag(tag)
    else:
        n = 0

    # Number of days until next check increases by factor of 2 each time, maxing at 32 days
    new_tag_date = datetime.today() + timedelta(days=2**min(5,n))
    new_tag_date_str = new_tag_date.strftime(DATE_FORMAT_FW)
    new_redcap_tag = "redcap_" + str(n + 1) + "_" + new_tag_date_str
    session.add_tag(new_redcap_tag)    
        
def mv_all_sessions(src_project: ProjectOutput, dst_project: ProjectOutput) -> None:
    """Moves all non-empty sessions from one project to another"""
        f"Moving all non-empty sessions from {src_project.group}/{src_project.label} to "
        "{dst_project.group}/{dst_project.label}"
    )
    for session in src_project.sessions():
        if session.acquisitions():
            mv_session(session, dst_project)

def rename_duplicate_subject(subject: SubjectOutput, acq_df: pd.DataFrame()) -> None:
    """Renames a subject to <sub_label>_<n>, where n is lowest unused integer."""
    regex = '^' + subject.label + '_\d{3}$'
    dup_labels = acq_df[acq_df['subject.label'].str.contains(regex, regex=True)]['subject.label']
   
    if not dup_labels.empty:
        dup_ints = dup_labels.str.replace(f"{subject.label}_", "")
        max_int = pd.to_numeric(dup_ints).max()
        new_suffix = str(max_int + 1).zfill(3)
        new_label = f"{subject.label}_{new_suffix}" 
    else:
        new_label = f"{subject.label}_001"
    
    subject.update({'label':new_label})

def smarter_copy(acq_list, src_project: ProjectOutput, dst_project: ProjectOutput, client) -> None:
    """Since smart-copy can't copy to an existing project, this function smart-copies
    all acquisitions from acq_list to a tmp project, waits for it to complete, moves 
    the sessions to the existing project, checks that they exist in the destination project,
    then deletes the tmp."""
    to_copy_tag = f"to_copy_{dst_project.label}"
    tmp_project_label = f"{dst_project.group}_{dst_project.label}"
   
    columns = [
        'subject.label',
        'session.label',
        'session.timestamp'
    ]
    dst_df = create_view_df(dst_project, columns)

    if not dst_df.empty:
        dst_df['session.date'] = dst_df['session.timestamp'].str[:10]
        sub_label_set = set(dst_df['subject.label'].to_list())
    else:
        sub_label_set = set()
    
    for acq in acq_list:
        acq = acq.reload()
        if to_copy_tag not in acq.tags:
            acq.add_tag(to_copy_tag)

        # Create a new subject if subject and session already exist in dst_project
        subject = client.get_subject(acq.parents.subject)
        if subject.label in sub_label_set:
            sub_df = dst_df[dst_df['subject.label'] == subject.label]
            session = client.get_session(acq.parents.session)
            session_date = session.timestamp.strftime('%Y-%m-%d')
            if not sub_df[
                (sub_df['session.label'] == session.label) 
                & (sub_df['session.date'] != session_date)
            ].empty:
                rename_duplicate_subject(subject, dst_df) 

            
    tmp_project_id = smart_copy(
        src_project,
        'tmp',
        to_copy_tag,
        tmp_project_label,
        True)["project_id"]
    tmp_project = client.get_project(tmp_project_id)
    check_smartcopy_loop(tmp_project)
    mv_all_sessions(tmp_project, dst_project)
    check_copied_acq_exist(acq_list, dst_project)
    delete_project('tmp', tmp_project_label)
