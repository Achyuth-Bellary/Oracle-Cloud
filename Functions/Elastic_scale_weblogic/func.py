######################################################################################################
# This function STARTS or STOP ALL VMs with provided value for tag auto-scale-env 
# and add it to Load Balancer
# Function:  scale-weblogic-test
#
# Author  : Achyuth Naidu Bellary
# Version : 0.1.0
# Date    : 28-may-2025
#
######################################################################################################

import io
import json
import oci
import logging
import os
from datetime import datetime, timedelta 
import base64
import requests
from oci.resource_scheduler import ScheduleClient
import time
from concurrent.futures import ThreadPoolExecutor
from fdk import response

# Configure logging
logging.basicConfig(level=logging.INFO)

def log_it(msg,type="i"):
    if (type =="i"):
        logging.info(msg)
    elif (type == "d"):
        logging.debug(msg)
    else:
        logging.info(msg)
    print(msg)

def get_all_compartments(tenancy_id):
    """
    Recursively fetch all compartments under the tenancy.
    Returns a list of dictionaries with compartment name and id.
    """
    log_it("inside function get_all_compartments , id ={tenancy_id}")
    identity = oci.identity.IdentityClient(config={}, signer=signer)
    compartments = []
    def fetch_sub_compartments(parent_id):
        sub_compartments = identity.list_compartments(
            compartment_id=parent_id,
            compartment_id_in_subtree=True,
            access_level="ANY"
        ).data
        for comp in sub_compartments:
            if comp.lifecycle_state == "ACTIVE":
                compartments.append({
                    "id": comp.id,
                    "name": comp.name if hasattr(comp, "name") else comp.display_name
                })
    # Include tenancy root
    tenancy = identity.get_compartment(tenancy_id).data
    compartments.append({
        "id": tenancy.id,
        "name": tenancy.name if hasattr(tenancy, "name") else tenancy.display_name
    })
    fetch_sub_compartments(tenancy_id)
    log_it(compartments,"d")
    return compartments

def get_signer():
    signer = oci.auth.signers.get_resource_principals_signer()
    #signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner() # for local in jenkins
    signer.refresh_security_token()
    return  signer


signer=get_signer()

def get_private_ip(instance_id, compartment_id_instance):
    log_it(f"inside function 'get_private_ip' , instance_id={instance_id},{compartment_id_instance}")
    # Instantiate the necessary OCI clients
    compute_client = oci.core.ComputeClient(config={},signer=signer)
    network_client = oci.core.VirtualNetworkClient(config={},signer=signer)
    # List the VNIC attachments to get the VNIC ID of the instance
    attachments = compute_client.list_vnic_attachments(
        compartment_id=compartment_id_instance,
        instance_id=instance_id
    ).data
    # Handle case if no VNICs are found
    if not attachments:
        raise Exception(f"No VNIC attachments found for instance {instance_id}.")
    # Fetch the private IP of the instance from its VNIC
    vnic = network_client.get_vnic(attachments[0].vnic_id).data
    log_it(f"private IP: {vnic.private_ip}")
    return vnic.private_ip

def is_instance_in_backend(lb_client, lb_id, backend_set_name, private_ip, port):
    try:
        port = int(port)  # Convert port to integer
    except ValueError:
        raise ValueError(f"Invalid port value: {port}")
    # Check if the instance's private IP is already a backend
    log_it(f"is_instance_in_backend , lb_id={lb_id} , backend_set_name ={backend_set_name}")
    backends = lb_client.list_backends(lb_id, backend_set_name).data
    for backend in backends:
        if backend.ip_address == private_ip and backend.port == port:
            return True
    return False

def add_instance_to_lb(lb_id, backend_set_name, instance_id, compartment_id_instance, port):
    log_it(f"Inside function add_instance_to_lb, lb_id={lb_id}")
    try:
        port = int(port)  # Convert port to integer
    except ValueError:
        raise ValueError(f"Invalid port value: {port}")
    # Initialize the Load Balancer client
    lb_client = oci.load_balancer.LoadBalancerClient(config={},signer=signer)
    # Get the instance's private IP using the correct compartment ID for the instance
    private_ip = get_private_ip( instance_id, compartment_id_instance)
    # Check if the instance is already added to the backend set
    if is_instance_in_backend(lb_client, lb_id, backend_set_name, private_ip, port):
        log_it(f"[WARN] Instance {instance_id} (IP: {private_ip}) is already in the backend set.")
        return f"[WARN] Instance {instance_id} (IP: {private_ip}) is already in the backend set."
    # Create backend details for the load balancer
    backend_details = oci.load_balancer.models.BackendDetails(
        ip_address=private_ip,
        port=port,
        weight=1  # Default weight
    )
    try:
        log_it(f"Adding to Backend {backend_set_name}")
        response = lb_client.create_backend(
            load_balancer_id=lb_id,
            backend_set_name=backend_set_name,
            create_backend_details=backend_details
        )
        log_it("Response status:", response.status)
        if response.status >= 200 and response.status < 300:
            log_it(f"[SUCCESS] Instance {instance_id} (IP: {private_ip}) added to load balancer.")
            return f"[SUCCESS] Instance {instance_id} (IP: {private_ip}) added to load balancer."
        else:
            log_it(f"[FAILED] Instance {instance_id} (IP: {private_ip}) added to load balancer, reponse= {response.status}")
            return f"[SUCCESS] Instance {instance_id} (IP: {private_ip}) added to load balancer, reponse= {response.status}"
    except Exception as e:
            log_it(f"[ERROR] Failed to add backend: {str(e)}")
            return f"[ERROR] Failed to add backend: {str(e)}"
    

def get_vm_names_and_ids_by_tags( comp_id, freeform_tag_filters={}):
    """Returns VM names and OCIDs for VMs matching the provided tags."""
    log_it("Inside function add_instance_to_lb")
    compute_client = oci.core.ComputeClient(config={},signer=signer)
    matched = []
    instances = compute_client.list_instances(compartment_id=comp_id).data
    for instance in instances:
        freeform_tags = instance.freeform_tags or {}
        # Check if the instance matches the tag filters
        if all(freeform_tags.get(k) == v or v == "*" for k, v in freeform_tag_filters.items()):
            matched.append({
                "name": instance.display_name,
                "ocid": instance.id,
                "port": freeform_tags.get("auto-scale-port"),
                "backend": freeform_tags.get("auto-scale-backend"),
                "lb_ocid": freeform_tags.get("auto-scale-lb-ocid"),
            })
            log_it(f"Instance {instance.display_name} matches tags.")
        else:
            log_it(f"Instance {instance.display_name} does not match tags.")
    return matched

def start_stop_vm(instance_id, instance_name, action):
    action = action.upper()
    try:
        compute_client = oci.core.ComputeClient(config={}, signer=signer)
        instance = compute_client.get_instance(instance_id).data
        pre_status = instance.lifecycle_state
        if action == "START" and pre_status != "RUNNING":
            compute_client.instance_action(instance_id, "START")
            oci.wait_until(compute_client, compute_client.get_instance(instance_id), 'lifecycle_state', 'RUNNING', max_wait_seconds=120, max_interval_seconds=10)
            status = f"Instance {instance.display_name} started successfully."
        elif action == "STOP" and pre_status != "STOPPED":
            compute_client.instance_action(instance_id, "STOP")
            oci.wait_until(compute_client, compute_client.get_instance(instance_id), 'lifecycle_state', 'STOPPED', max_wait_seconds=120, max_interval_seconds=10)
            status = f"Instance {instance.display_name} stopped successfully."
        elif action == "STATUS":
            status = f"Instance {instance.display_name} STATUS is {pre_status}"
        else:
            status = f"Instance {instance.display_name} is already {pre_status.lower()}."
        return {
            "instance_name": instance.display_name,
            "instance_id": instance_id,
            "action": action,
            "pre_status": pre_status,
            "post_status": instance.lifecycle_state,
            "status": status
        }
    except Exception as e:
        logging.error(f"Error occurred while processing VM {instance_id}: {str(e)}")
        return {
            "instance_name": instance_name,
            "instance_id": instance_id,
            "action": action,
            "error": str(e)  # Include the exception message in the error field
        }

def email_message(alarm_payload, initial_node_count, final_node_count, status_message):
    alarm_id = body = title = alarm_body = dedupekey = alarm_timestamp = ""
    if alarm_payload is not None:
        if "title" in alarm_payload:
            title = alarm_payload["title"]
            logging.info("Title: " + title)
        if "body" in alarm_payload:
            alarm_body = alarm_payload["body"]
            logging.info("Body: " + body)
        if "dedupeKey" in alarm_payload:
            dedupekey = alarm_payload["dedupeKey"]
            logging.info('Dedupe key = ' + dedupekey)
        if "timestampEpochMillis" in alarm_payload:
            time_in_millis = alarm_payload["timestampEpochMillis"] / 1000.0
            alarm_timestamp = datetime.fromtimestamp(time_in_millis).strftime('%Y-%m-%d %H:%M:%S')
            logging.info('Alarm timestamp = ' + alarm_timestamp)
        if "alarmMetaData" in alarm_payload:
            alarmMetadataList = alarm_payload['alarmMetaData']
            if len(alarmMetadataList) > 0:
                alarm_id = alarmMetadataList[0]['id']
    body_msg = """
        Alarm Name: {0}
        Alarm Body: {1}
        Time: {2}
        Scale Out Node count  {3} -> {4}
        Status: {5}
        """.format(title, alarm_body, alarm_timestamp,
                   initial_node_count, final_node_count, status_message)
    return body_msg

def send_email(signer, topic_id, email_body=None, subject=""):
    """
    Sends an email to the email notification topic upon completion of the scaling function.
    :param signer:
    :param topic_id:
    :param email_body:
    :param subject
    :return:
    """
    logging.info(f"[DEBUG] Sending email with subject: {subject}, topic_id: {topic_id}")
    logging.info(f"[DEBUG] Email body: {email_body}")
    ons_client = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)
    try:
        message_details = oci.ons.models.MessageDetails(
            body=email_body,
            title=subject)
        publish_message_response = ons_client.publish_message(topic_id, message_details=message_details,
                                                              message_type="RAW_TEXT")
        logging.info(f"[DEBUG] Email sent successfully. Response: {publish_message_response}")
    except(Exception, ValueError) as ex:
        logging.exception("ERROR: sending confirmation email failed: {0}".format(ex))

def get_secret(secret_id):
    """
    Retrieves a secret from OCI Vault.
    :param secret_id: OCID of the secret
    :return: The secret content as a string
    """
    try:
        secrets_client = oci.secrets.SecretsClient(config={}, signer=signer)
        secret_bundle = secrets_client.get_secret_bundle(secret_id).data
        secret_content = base64.b64decode(secret_bundle.secret_bundle_content.content).decode("utf-8")
        return secret_content
    except Exception as e:
        logging.error(f"[ERROR] Failed to retrieve secret from OCI Vault. Error: {str(e)}")
        raise

def check_weblogic_server_state(weblogic_host, server_name, username, password):
    """
    Checks the state of a WebLogic server using its REST API.
    :param weblogic_host: WebLogic Admin Server host (e.g., https://<host>:<port>)
    :param server_name: Name of the WebLogic server to check
    :param username: WebLogic Admin username
    :param password: WebLogic Admin password
    :return: True if the server is RUNNING, False otherwise
    """
    try:
        url = f"https://{weblogic_host}/management/weblogic/latest/domainRuntime/serverLifeCycleRuntimes/{server_name}"
        headers = {
            "Authorization": "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode(),
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        if response.status_code == 200:
            server_state = response.json().get("state", "").upper()
            if server_state == "RUNNING":
                logging.info(f"[INFO] WebLogic server {server_name} is RUNNING.")
                return True
            else:
                logging.warn(f"[WARN] WebLogic server {server_name} is not running. Current state: {server_state}")
                return False
        else:
            logging.error(f"[ERROR] Failed to fetch WebLogic server state. HTTP Status: {response.status_code}")
            return False
    except Exception as e:
        logging.error(f"[ERROR] Exception occurred while checking WebLogic server state: {str(e)}")
        return False

def schedule_follow_up(auto_scale_env, lb_id, compartment_id, function_id ):
    """
    Schedule a one-time follow-up function to check the load balancer's health after 15 minutes.
    """
    try:
        # Initialize the Resource Scheduler client
        resource_scheduler_client = oci.resource_scheduler.ScheduleClient(config={}, signer=signer)
        # Calculate the time 15 minutes from now
        current_time = datetime.utcnow()
        scheduled_time = current_time + timedelta(minutes=15)
        # Define the schedule details
        schedule_details = oci.resource_scheduler.models.CreateScheduleDetails(
        compartment_id=compartment_id,  # Replace with your compartment OCID
        action="START_RESOURCE",
        recurrence_type="ICAL",  # Use ICAL for iCalendar format
        recurrence_details="FREQ=DAILY;COUNT=1",  # One-time schedule
        display_name="Check Load Balancer Health",
        description="Follow-up to check the health of the load balancer",
        resources=[
            oci.resource_scheduler.models.Resource(
                id=function_id, 
                metadata={
                    "auto_scale_env": auto_scale_env,
                    "lb_id": lb_id,
                    "compartment_id": compartment_id
                },
                parameters=[
                    oci.resource_scheduler.models.BodyParameter(
                        parameter_type="BODY",
                        value=json.dumps({
                            "auto_scale_env": auto_scale_env,
                            "lb_id": lb_id,
                            "compartment_id": compartment_id,
                            })
                        )
                    ],
                )
            ],
            time_starts=scheduled_time.strftime("%Y-%m-%dT%H:%M:%SZ")
        )   
        # Create the schedule
        create_schedule_response = resource_scheduler_client.create_schedule(create_schedule_details=schedule_details)
        logging.info(f"[INFO] Scheduled follow-up job to check load balancer health. Response: {create_schedule_response.data}")
    except Exception as e:
        logging.error(f"[ERROR] Failed to schedule follow-up job. Error: {str(e)}")

def drain_backend(lb_client, lb_id, backend_set_name, private_ip, port, timeout=240, interval=5):
    """
    Drains traffic from the backend by setting its weight to 1 and waits until it is drained.
    """
    log_it(f"Draining backend {private_ip}:{port} in backend set {backend_set_name}")
    try:
        # Initiate the drain operation
        lb_client.update_backend(
            load_balancer_id=lb_id,
            backend_set_name=backend_set_name,
            backend_name=f"{private_ip}:{port}",
            update_backend_details=oci.load_balancer.models.UpdateBackendDetails(
                weight=1,
                backup=False,
                drain=True,
                offline=False
            )
        )
        log_it(f"[INFO] Drain initiated for backend {private_ip}:{port}. Checking status...")
        # Poll for drain status
        elapsed_time = 0
        while elapsed_time < timeout:
            backends = lb_client.list_backends(lb_id, backend_set_name).data
            for backend in backends:
                if backend.ip_address == private_ip and backend.port == port:
                    if not backend.drain:  # Check if the backend is still draining
                        log_it(f"[INFO] Backend {private_ip}:{port} is still draining. Waiting...")
                        time.sleep(interval)
                        elapsed_time += interval
                        break
                    else:
                        log_it(f"[SUCCESS] Backend {private_ip}:{port} drained successfully.")
                        return True
            else:
                log_it(f"[ERROR] Backend {private_ip}:{port} not found in backend set {backend_set_name}.")
                return False
        log_it(f"[ERROR] Timeout reached while draining backend {private_ip}:{port}.")
        return False
    except Exception as e:
        log_it(f"[ERROR] Failed to drain backend {private_ip}:{port}. Error: {str(e)}")
        return False

def mark_backend_offline(lb_client, lb_id, backend_set_name, private_ip, port, timeout=240, interval=5):
    """
    Marks the backend as offline and waits until it is fully offline, but only if it is already draining.
    """
    log_it(f"Marking backend {private_ip}:{port} as offline in backend set {backend_set_name}")
    try:
        # Check if the backend is already draining
        backends = lb_client.list_backends(lb_id, backend_set_name).data
        for backend in backends:
            if backend.ip_address == private_ip and backend.port == port:
                if not backend.drain:  # Backend is not draining
                    log_it(f"[ERROR] Backend {private_ip}:{port} is not draining. Skipping offline operation.")
                    return False
                if backend.offline:  # Backend is already offline
                    log_it(f"[INFO] Backend {private_ip}:{port} is already offline. Skipping offline operation.")
                    return True
                break
        else:
            log_it(f"[ERROR] Backend {private_ip}:{port} not found in backend set {backend_set_name}.")
            return False
        # Initiate the offline operation
        lb_client.update_backend(
            load_balancer_id=lb_id,
            backend_set_name=backend_set_name,
            backend_name=f"{private_ip}:{port}",
            update_backend_details=oci.load_balancer.models.UpdateBackendDetails(
                weight=1,  # Set weight to 0 to stop traffic
                backup=False,
                drain=True,
                offline=True
            )
        )
        log_it(f"[INFO] Offline operation initiated for backend {private_ip}:{port}. Checking status...")
        # Poll for offline status
        elapsed_time = 0
        while elapsed_time < timeout:
            backends = lb_client.list_backends(lb_id, backend_set_name).data
            for backend in backends:
                if backend.ip_address == private_ip and backend.port == port:
                    if not backend.offline:  # Backend is not yet offline
                        log_it(f"[INFO] Backend {private_ip}:{port} is still online. Waiting...")
                        time.sleep(interval)
                        elapsed_time += interval
                        break
                    else:  # Backend is fully offline
                        log_it(f"[SUCCESS] Backend {private_ip}:{port} marked as offline successfully.")
                        return True
            else:
                log_it(f"[ERROR] Backend {private_ip}:{port} not found in backend set {backend_set_name}.")
                return False
        log_it(f"[ERROR] Timeout reached while marking backend {private_ip}:{port} as offline.")
        return False
    except Exception as e:
        log_it(f"[ERROR] Failed to mark backend offline {private_ip}:{port}. Error: {str(e)}")
        return False

def remove_backend(lb_client, lb_id, backend_set_name, private_ip, port, timeout=240, interval=5):
    """
    Removes the backend from the backend set only if it is already offline.
    """
    log_it(f"Removing backend {private_ip}:{port} from backend set {backend_set_name}")
    try:
        # Check if the backend is offline
        backends = lb_client.list_backends(lb_id, backend_set_name).data
        for backend in backends:
            if backend.ip_address == private_ip and backend.port == port:
                if not backend.offline:  # Backend is not offline
                    log_it(f"[ERROR] Backend {private_ip}:{port} is not offline. Skipping removal.")
                    return False
                break
        else:
            log_it(f"[ERROR] Backend {private_ip}:{port} not found in backend set {backend_set_name}.")
            return False
        # Initiate the remove operation
        lb_client.delete_backend(
            load_balancer_id=lb_id,
            backend_set_name=backend_set_name,
            backend_name=f"{private_ip}:{port}"
        )
        log_it(f"[INFO] Remove operation initiated for backend {private_ip}:{port}. Checking status...")
        # Poll for removal status
        elapsed_time = 0
        while elapsed_time < timeout:
            backends = lb_client.list_backends(lb_id, backend_set_name).data
            backend_exists = False
            for backend in backends:
                if backend.ip_address == private_ip and backend.port == port:
                    backend_exists = True
                    log_it(f"[INFO] Backend {private_ip}:{port} still exists. Waiting...")
                    time.sleep(interval)
                    elapsed_time += interval
                    break
            if not backend_exists:  # Backend has been successfully removed
                log_it(f"[SUCCESS] Backend {private_ip}:{port} removed successfully.")
                return True
        log_it(f"[ERROR] Timeout reached while removing backend {private_ip}:{port}.")
        return False
    except Exception as e:
        log_it(f"[ERROR] Failed to remove backend {private_ip}:{port}. Error: {str(e)}")
        return False

def run_command_on_vm(instance_id, compartment_id, command_content, timeout=240, interval=5):
    """
    Executes a command on the VM using OCI Compute Instance Agent.
    :param instance_id: OCID of the instance
    :param compartment_id: OCID of the compartment
    :param command_content: Command to execute on the VM
    :param timeout: Maximum time to wait for command execution (in seconds)
    :param interval: Polling interval to check command status (in seconds)
    :return: True if the command executes successfully, False otherwise
    """
    try:
        compute_instance_agent_client = oci.compute_instance_agent.ComputeInstanceAgentClient(config={}, signer=signer)
        create_instance_agent_command_response = compute_instance_agent_client.create_instance_agent_command(
            create_instance_agent_command_details=oci.compute_instance_agent.models.CreateInstanceAgentCommandDetails(
                compartment_id=compartment_id,
                execution_time_out_in_seconds=timeout,
                target=oci.compute_instance_agent.models.InstanceAgentCommandTarget(
                    instance_id=instance_id),
                content=oci.compute_instance_agent.models.InstanceAgentCommandContent(
                    source=oci.compute_instance_agent.models.InstanceAgentCommandSourceViaTextDetails(
                        source_type="TEXT",
                        text=command_content),
                    output=oci.compute_instance_agent.models.InstanceAgentCommandOutputViaTextDetails(
                        output_type="TEXT")),
                display_name="RunCommand-updateHostNames"))
        command_id = create_instance_agent_command_response.data.id
        elapsed_time = 0
        # Wait for the command to complete
        while elapsed_time < timeout:
            command_status = compute_instance_agent_client.get_instance_agent_command_execution(
                instance_id=instance_id,
                instance_agent_command_id=command_id
            ).data.lifecycle_state
            if command_status == "SUCCEEDED":
                log_it(f"[INFO] Command executed successfully on VM {instance_id}.")
                return True
            elif command_status in ["FAILED", "CANCELED"]:
                log_it(f"[ERROR] Command execution failed on VM {instance_id}. Status: {command_status}")
                return False
            time.sleep(interval)
            elapsed_time += interval
        log_it(f"[ERROR] Command execution timed out on VM {instance_id}.")
        return False
    except oci.exceptions.ServiceError as e:
        log_it(f"[ERROR] OCI Service Error while executing command on VM {instance_id}. Error: {str(e)}")
        return False
    except Exception as e:
        log_it(f"[ERROR] Failed to execute command on VM {instance_id}. Error: {str(e)}")
        return False

def scale_down_vm(vm, compartment_id):
    """
    Handles the scale-down process for a single VM.
    Optimized to combine service stop commands and reduce execution time.
    """
    no_op_lb = []
    no_op_vms = {
        "already_running": [],  # VMs already in the RUNNING state
        "already_stopped": [],  # VMs already in the STOPPED state
    }
    try:
        lb_client = oci.load_balancer.LoadBalancerClient(config={}, signer=signer)
        compute_client = oci.core.ComputeClient(config={}, signer=signer)
        # Check VM state
        instance = compute_client.get_instance(vm['ocid']).data
        if instance.lifecycle_state == "STOPPED":
            log_it(f"[INFO] VM {vm['name']} is already in STOPPED state. Skipping scale-down.")
            no_op_vms["already_stopped"].append(vm['name'])  # Add to no-op list
            return {"vm_name": vm['name'], "status": "no-op", "reason": "VM already stopped"}
        # Check if VM is in the load balancer
        private_ip = get_private_ip(vm['ocid'], compartment_id)
        backends = lb_client.list_backends(vm['lb_ocid'], vm['backend']).data
        # Check backend count to determine if elastic VMs are present
        if len(backends) <= 3:
            log_it(f"[INFO] Load balancer {vm['lb_ocid']} has only {len(backends)} backends. Assuming main servers. Skipping scale-down for VM {vm['name']}.")
            no_op_lb.append(vm['name'])  # Add to no-op LB list
            return {"vm_name": vm['name'], "status": "no-op", "reason": "Main server in load balancer"}
        if not is_instance_in_backend(lb_client, vm['lb_ocid'], vm['backend'], private_ip, int(vm['port'])):
            log_it(f"[INFO] VM {vm['name']} is not part of the load balancer. Skipping scale-down.")
            return {"vm_name": vm['name'], "status": "no-op", "reason": "VM not in load balancer"}
        # Proceed with scale-down process
        # Stage 1: Drain traffic
        if not drain_backend(lb_client, vm['lb_ocid'], vm['backend'], private_ip, int(vm['port'])):
            return {"vm_name": vm['name'], "status": "failure", "reason": "Failed to drain backend"}
        # Stage 2: Mark backend as offline
        if not mark_backend_offline(lb_client, vm['lb_ocid'], vm['backend'], private_ip, int(vm['port'])):
            return {"vm_name": vm['name'], "status": "failure", "reason": "Failed to mark backend offline"}
        # Stage 3: Remove backend
        if not remove_backend(lb_client, vm['lb_ocid'], vm['backend'], private_ip, int(vm['port'])):
            return {"vm_name": vm['name'], "status": "failure", "reason": "Failed to remove backend"}
        # Stage 4: Stop the services on the VM in one go
        #combined_command = "sudo systemctl stop wls-managedserver.service && sudo systemctl stop wls-nodemanager.service"
        #if not run_command_on_vm(vm['ocid'], compartment_id, combined_command):
        #   return {"vm_name": vm['name'], "status": "failure", "reason": "Failed to stop services on VM"}
        # Stage 5: Power off the VM
        vm_action_result = start_stop_vm(vm['ocid'], vm['name'], "STOP")
        if "successfully" in vm_action_result.get("status", "").lower():
            return {"vm_name": vm['name'], "status": "success"}
        else:
            return {"vm_name": vm['name'], "status": "failure", "reason": vm_action_result.get("error", "Unknown error")}
    except Exception as e:
        return {"vm_name": vm['name'], "status": "failure", "reason": str(e)}
    
def log_summary_to_nosql(nosql_client, table_name, table_compartment_id, action, environment, total_vms, success_count, failure_count, no_op_count, overall_status):
    """
    Logs a summary of the scale action into the NoSQL table.
    """
    try:
        log_entry = {
            "Action": action,
            "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            "Environment": environment,
            "Total_VMs": total_vms,
            "Success_Count": success_count,
            "Failure_Count": failure_count,
            "No_Op_Count": no_op_count,
            "Overall_Status": overall_status
        }
        update_row_response = nosql_client.update_row(
        table_name_or_id=table_name,
        update_row_details=oci.nosql.models.UpdateRowDetails(
        value=log_entry,
        compartment_id=table_compartment_id))
        log_it(f"[INFO] Logged summary for action {action} in environment {environment}. Overall Status: {overall_status}")
    except Exception as e:
        log_it(f"[ERROR] Failed to log summary for action {action}. Error: {str(e)}")

def get_last_scale_action(nosql_client, table_name, environment, compartment_id):
    """
    Queries the NoSQL table for all scale actions for the given environment,
    then returns the action from the record with the latest Timestamp.
    """
    query_statement = (
        f"SELECT Action, Timestamp FROM {table_name} "
        f"WHERE Environment = '{environment}'"
    )
    try:
        query_response = nosql_client.query(
            query_details=oci.nosql.models.QueryDetails(
                compartment_id=compartment_id, 
                statement=query_statement
            )
        )
        # Extract items from the response (the data is in the "items" key)
        rows = query_response.data.items
        if not rows:
            log_it(f"[INFO] No previous scale action found for environment {environment}.")
            return None
        # Sort rows by Timestamp (assuming ISO format) in descending order
        rows_sorted = sorted(rows, key=lambda x: x.get("Timestamp", ""), reverse=True)
        log_it(f"[INFO] Last scale action for environment {environment} retrieved successfully.")
        log_it(f"[DEBUG] Sorted Query response: {rows_sorted}")
        return rows_sorted[0]
    except Exception as e:
        log_it(f"[ERROR] Failed to query last scale action: {str(e)}")
    return None

def handler(ctx, data: io.BytesIO = None):
    logs = []
    output = []
    success_vms_state = []  # VMs successfully started/stopped
    success_vms_lb = []  # VMs successfully added/removed from the Load Balancer
    failed_vms = []  # Track failed VMs
    no_op_vms = {
        "already_running": [],  # VMs already in the RUNNING state
        "already_stopped": [],  # VMs already in the STOPPED state
    }
    no_op_lb = []  # Track No-Op Load Balancer details
    auto_scale_env = ""
    alarm_payload = None  # Initialize alarm_payload

    try:
        # Parse input data
        body = json.loads(data.getvalue())
        parsed_body = json.loads(body.get("body", "{}"))
        auto_scale_env = parsed_body.get("auto_scale_env")
        action = parsed_body.get("action").upper()  # Convert action to uppercase
        compartment_id = os.environ.get("KEY_COMPARTMENT_OCID")
        function_id = os.environ.get("CHECK_LOAD_BALANCER_HEALTH_OCID")
        table_compartment_id = os.environ.get("TABLE_COMPARTMENT_OCID")
        table_name = os.environ.get("TABLE_NAME")  # NoSQL table name for logging

        # Retrieve WebLogic credentials from OCI Vault
        weblogic_host = os.environ.get("WEBLOGIC_HOST")  # WebLogic Admin Server host
        username = os.environ.get("WEBLOGIC_USERNAME")  # Secret OCID for username
        password_secret_id = os.environ.get("WEBLOGIC_PASSWORD_SECRET_OCID")  # Secret OCID for password

        password = get_secret(password_secret_id)

        if (action == "START"):
            # Check WebLogic Admin Server state
            admin_server_name = os.environ.get("ADMIN_SERVER_NAME")  # Admin Server name
            if not check_weblogic_server_state(weblogic_host, admin_server_name, username, password):
                logs.append("[ERROR] WebLogic Admin Server is not running. Aborting operations.")
                return response.Response(ctx, response_data=json.dumps({"error": "WebLogic Admin Server is not running", "logs": logs}), headers={"Content-Type": "application/json"})

        logs.append(f"auto_scale_env={auto_scale_env}, action={action}, compartment_id={compartment_id}")
        logging.info(f"auto_scale_env={auto_scale_env}, action={action}, compartment_id={compartment_id}")

        # Define tag filters
        freeform_tag_filters = {
            "auto-scale": "enabled",
            "auto-scale-env": auto_scale_env,
            "auto-scale-backend": "*",
            "auto-scale-lb-ocid": "*",
            "auto-scale-port": "*"
        }

        # Fetch VMs matching the tags
        vm_list = get_vm_names_and_ids_by_tags(compartment_id, freeform_tag_filters)
        logging.info(f"vm_list={vm_list}")
        logs.append(f"vm_list={vm_list}")

        if action == "STOP":
            # Initialize NoSQL client and table variables if not already done
            nosql_client = oci.nosql.NosqlClient(config={}, signer=signer)
            last_record = get_last_scale_action(nosql_client, table_name, auto_scale_env, table_compartment_id)
            last_action = last_record.get("Action")
            last_timestamp = last_record.get("Timestamp")
            logs.append(f"[DEBUG] Last scale action: {last_action}, Timestamp: {last_timestamp}")
            log_it(f"[DEBUG] Last scale action: {last_action}, Timestamp: {last_timestamp}")
            if not last_record or last_record.get("Action") != "START":
                log_it(f"[INFO] Recent scale action is not START. Skipping STOP to avoid concurrent stop operations.")
                logs.append("[INFO] Recent scale action is not START. Skipping STOP to avoid concurrent stop operations.")
                return response.Response(ctx, response_data=json.dumps({"logs": logs, "output": []}), headers={"Content-Type": "application/json"})
            try:
                # Try parsing with fractional seconds; adjust format as needed
                parsed_ts = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError:
                parsed_ts = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%SZ")
            
            if (datetime.utcnow() - parsed_ts) < timedelta(hours=1):
                log_it("[INFO] Last START action was performed within the last hour. Skipping STOP action to allow sufficient time.")
                logs.append("[INFO] Last START action was performed within the last hour. Skipping STOP action to allow sufficient time.")
                return response.Response(ctx, response_data=json.dumps({"logs": logs, "output": []}), headers={"Content-Type": "application/json"})
                
            # Proceed with STOP operations using multithreading as before
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = executor.map(lambda vm: scale_down_vm(vm, compartment_id), vm_list)
                for vm_action_result in results:
                    output.append(vm_action_result)
                    if vm_action_result["status"] == "success":
                        logs.append(f"[INFO] VM {vm_action_result['vm_name']} scaled down successfully.")
                        success_vms_state.append(vm_action_result["vm_name"])
                    else:
                        logs.append(f"[WARN] Failed to scale down VM {vm_action_result['vm_name']}. Reason: {vm_action_result['reason']}")
                        failed_vms.append({"vm_name": vm_action_result["vm_name"], "reason": vm_action_result["reason"]})
                        
        elif action == "START":
            # Initialize NoSQL client and table variables for START actions
            nosql_client = oci.nosql.NosqlClient(config={}, signer=signer)
            last_record = get_last_scale_action(nosql_client, table_name, auto_scale_env, table_compartment_id)
            last_action = last_record.get("Action")
            if last_action == "START":
                logs.append("[INFO] Recent scale action is already START. Skipping START to avoid concurrent start operations.")
                log_it(f"[INFO] Recent scale action is already START. Skipping START to avoid concurrent start operations.")
                return response.Response(ctx, response_data=json.dumps({"logs": logs, "output": []}), headers={"Content-Type": "application/json"})
            
            # Process each VM for scale-up if no recent START action was recorded
            for vm in vm_list:
                try:
                    # Perform the start operation
                    vm_action_result = start_stop_vm(vm['ocid'], vm['name'], action)
                    output.append(vm_action_result)
                    logs.append(f"[DEBUG] VM Action Result for {vm['name']}: {vm_action_result}")
                    pre_status = vm_action_result.get("pre_status", "").upper()
                    post_status = vm_action_result.get("post_status", "").upper()
                    status_message = vm_action_result.get("status", "").lower()
                    if action == "START" and pre_status == "RUNNING":
                        logs.append(f"[INFO] VM {vm['name']} is already running. Skipping start operation.")
                        no_op_vms["already_running"].append(f"{vm['name']} (Status: {post_status})")
                    elif "successfully" in status_message:
                        logs.append(f"[INFO] VM {vm['name']} started successfully.")
                        success_vms_state.append(vm['name'])
                    else:
                        error_message = vm_action_result.get("error", "Unexpected error occurred")
                        failed_vms.append({"vm_name": vm['name'], "reason": error_message})
                        logs.append(f"[WARN] Failed to start VM {vm['name']}. Error: {error_message}")
                        continue  # Skip adding to the load balancer if start fails
                        
                    # Add instance to Load Balancer
                    out_lb_add = add_instance_to_lb(vm['lb_ocid'], vm['backend'], vm['ocid'], compartment_id, vm['port'])
                    output.append(out_lb_add)
                    if "success" in out_lb_add.lower():
                        logs.append(f"[INFO] VM {vm['name']} added to Load Balancer successfully.")
                        success_vms_lb.append(f"{vm['name']} (LB: {vm['backend']}, Port: {vm['port']})")
                    elif "already in the backend set" in out_lb_add.lower():
                        logs.append(f"[INFO] VM {vm['name']} is already part of the Load Balancer. Skipping addition.")
                        no_op_lb.append(f"{vm['name']} (LB: {vm['backend']}, Port: {vm['port']})")
                    else:
                        failed_vms.append({"vm_name": vm['name'], "reason": out_lb_add})
                        logs.append(f"[WARN] Failed to add VM {vm['name']} to the Load Balancer. Response: {out_lb_add}")
                except Exception as vm_error:
                    failed_vms.append({"vm_name": vm['name'], "reason": str(vm_error)})
                    logs.append(f"[ERROR] Failed to process VM {vm['name']}. Error: {str(vm_error)}")

        # Schedule follow-up function
        if success_vms_lb:
            schedule_follow_up(auto_scale_env, vm_list[0]['lb_ocid'], compartment_id, function_id)

        # Initialize NoSQL client
        nosql_client = oci.nosql.NosqlClient(config={}, signer=signer) 
        

        # After processing all VMs
        total_vms = len(vm_list)
        success_count = len(success_vms_state)
        failure_count = len([vm for vm in failed_vms if vm["vm_name"] not in no_op_vms["already_stopped"] and vm["vm_name"] not in no_op_lb])
        no_op_count = len(no_op_vms["already_running"]) + len(no_op_vms["already_stopped"]) + len(no_op_lb)

        # Determine overall status
        if failure_count == 0 and success_count > 0 and no_op_count >= 0:
            overall_status = "Success"
        elif failure_count > 0 and success_count > 0 and no_op_count >= 0:
            overall_status = "Partial Success"
        elif no_op_count == total_vms:
            overall_status = "No Operation"
        else:
            overall_status = "Failure"

        # Log the summary to NoSQL
        log_summary_to_nosql(
            nosql_client,
            table_name,
            table_compartment_id,
            action=action,
            environment=auto_scale_env,
            total_vms=total_vms,
            success_count=success_count,
            failure_count=failure_count,
            no_op_count=no_op_count,
            overall_status=overall_status
        )

        # Construct email content
        notification_topic_id = os.environ.get("wlsc_email_notification_topic_id")
        if notification_topic_id:
            if success_vms_state or success_vms_lb or failed_vms or no_op_vms["already_running"] or no_op_vms["already_stopped"] or no_op_lb:
                subject = f"Auto Scale INT servers processing - {auto_scale_env} - {action} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                email_body = email_message(
                    alarm_payload=body,
                    initial_node_count=len(success_vms_state),
                    final_node_count=len(success_vms_state) + len(failed_vms),
                    status_message=f"Success: {len(success_vms_state) + len(success_vms_lb)}, Failures: {len(failed_vms)}, No-Ops: {len(no_op_vms['already_running']) + len(no_op_vms['already_stopped']) + len(no_op_lb)}"
                )
                email_body += f"\n\nAction Performed: {'Scale-Up (START)' if action == 'START' else 'Scale-Down (STOP)'}"
                if success_vms_state:
                    email_body += "\n\nSuccessfully Processed VMs (State Changes):\n"
                    email_body += "\n".join(success_vms_state)
                if success_vms_lb:
                    email_body += "\n\nSuccessfully Processed VMs (Load Balancer Additions):\n"
                    email_body += "\n".join(success_vms_lb)
                if no_op_vms["already_running"]:
                    email_body += "\n\nNo-Op VMs (Already Running):\n"
                    email_body += "\n".join(no_op_vms["already_running"])
                if no_op_vms["already_stopped"]:
                    email_body += "\n\nNo-Op VMs (Already Stopped):\n"
                    email_body += "\n".join(no_op_vms["already_stopped"])
                if no_op_lb:
                    email_body += "\n\nNo-Op Load Balancer VMs:\n"
                    email_body += "\n".join(no_op_lb)
                if failed_vms:
                    email_body += "\n\nFailed VMs:\n"
                    for failure in failed_vms:
                        reason = failure.get("reason", "No specific reason provided")
                        email_body += f"- {failure['vm_name']}: {reason}\n"
                send_email(
                    signer=signer,
                    topic_id=notification_topic_id,
                    email_body=email_body,
                    subject=subject
                )

        return response.Response(ctx, response_data=json.dumps({"output": output, "logs": logs}), headers={"Content-Type": "application/json"})

    except Exception as e:
        logs.append(f"[ERROR] {str(e)}")
        notification_topic_id = os.environ.get("wlsc_email_notification_topic_id")
        if notification_topic_id:
            email_body = f"An error occurred during VM processing: {str(e)}"
            send_email(
                signer=oci.auth.signers.get_resource_principals_signer(),
                topic_id=notification_topic_id,
                email_body=email_body,
                subject=f"Auto Scale INT servers Error- {auto_scale_env}"
            )
        return response.Response(ctx, response_data=json.dumps({"error": str(e), "logs": logs}), headers={"Content-Type": "application/json"})

