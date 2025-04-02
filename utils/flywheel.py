#!/usr/bin/env python

import flywheel
from flywheel import Gear, SessionListOutput, ProjectOutput
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import logging

log = logging.getLogger(__name__)


def check_smartcopy_job_complete(dst_project: ProjectOutput) -> bool:
    """Check if a smart copy job is complete."""
    copy_status = dst_project.reload().copy_status
    if copy_status == flywheel.ProjectCopyStatus.COMPLETED:
        return True
    elif copy_status == flywheel.ProjectCopyStatus.FAILED:
        raise RuntimeError(f"Smart copy job to project {dst_project} failed")
    else:
        return False

def create_view_df(container, columns: list, client, filter=None, container_type='acquisition'):
    """Get unique labels for all acquisitions in the container.

    This is done using a single Data View which is more efficient than iterating through
    all acquisitions, sessions, and subjects. This prevents time-out errors in large projects.
    """

    builder = flywheel.ViewBuilder(
        container=container_type,
        filename="*.*",
        match='all',
        filter=filter,
        process_files=False,
        include_ids=False,
        include_labels=False
    )
    for c in columns:
        builder.column(src=c)
   
    view = builder.build()
    return client.read_view_dataframe(view, container.id)

def delete_project(group_id: str, project_label) -> None:
    """Deletes a project."""
    group = client.get_group(group_id)
    if group:
        project = group.projects.find_first(f"label={project_label}")
        if project:
            client.delete_project(project.id)
            log.info(f"Deleted project {group_id}/{project_label}")

def smart_copy(
    src_project: ProjectOutput,
    group_id: str = None,
    tag: str = None,
    dst_project_label: str = None,
    delete_existing_project = False) -> dict:
    """Smart copy a project to a group and returns API response."""

    if delete_existing_project:
        delete_project(client, group_id, dst_project_label)

    data = {
        "group_id": group_id,
        "project_label": dst_project_label,
        "filter": {
            "exclude_analysis": False,
            "exclude_notes": False,
            "exclude_tags": True,
            "include_rules": [],
            "exclude_rules": [],
        },
    }

    data["filter"]["include_rules"].append(f"acquisition.tags={tag}")
    log.info(
        f'Smart-copying acquisition labeled "{tag}" from "{src_project.label}" '
        'to "{group_id}/{dst_project_label}'
    )

    return client.project_copy(src_project.id, data)

def send_email(subject, html_content, sender, recipients, password):
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(recipients)
    msg.attach(MIMEText(html_content, 'html'))

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp_server:
       smtp_server.login(sender, password)
       smtp_server.sendmail(sender, recipients, msg.as_string())

    log.info(f"Email sent to {recipients}")

def run_gear(
    gear: Gear,
    inputs: dict,
    config: dict,
    dest,
    tags=None) -> str:
    """Submits a job with specified gear and inputs. dest can be any type of container
    that is compatible with the gear (project, subject, session, acquisition)"""
   
    for i in range(0,3):
        try:
            # Run the gear on the inputs provided, stored output in dest constainer and returns job ID
            gear_job_id = gear.run(inputs=inputs, config=config, destination=dest, tags=tags)
            log.debug('Submitted job %s', gear_job_id)
            return gear_job_id
        except flywheel.rest.ApiException:
            #log.exception('An exception was raised when attempting to submit a job for %s', gear.name)
            time.sleep(1)

def mv_session( session: SessionListOutput, dst_project: ProjectOutput) -> None:
    """Moves a session to another project."""
    try:
        session.update(project=dst_project.id)
    except flywheel.ApiException as exc:
        if exc.status == 422:
            sub_label = client.get_subject(session.parents.subject).label.replace(',', '\,')
            subject_dst_id = dst_project.subjects.find_first(f'label="{sub_label}"').id
            body = {
                "sources": [session.id],
                "destinations": [subject_dst_id],
                "destination_container_type": 'subjects',
                "conflict_mode": 'skip'
            }
            client.bulk_move_sessions(body=body)
        else:
            log.exception(
                f"Error moving subject {session.subject.label}/{session.label}"
                "from {src_project.label} to {dst_project.label}"
            )



