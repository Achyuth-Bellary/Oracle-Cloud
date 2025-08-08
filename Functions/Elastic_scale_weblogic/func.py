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

def log_it(msg, log_type="INFO", context=""):
    """
    Centralized logging function with consistent formatting.
    :param msg: The message to log
    :param log_type: Log level (INFO, DEBUG, WARN, ERROR)
    :param context: Optional context (function name, operation)
    """
    log_type = log_type.upper()
    
    # Format message with context if provided
    if context:
        formatted_msg = f"[{context}] {msg}"
    else:
        formatted_msg = msg
    
    # Log to appropriate level
    if log_type == "DEBUG":
        logging.debug(formatted_msg)
    elif log_type == "WARN" or log_type == "WARNING":
        logging.warning(formatted_msg)
    elif log_type == "ERROR":
        logging.error(formatted_msg)
    else:  # Default to INFO
        logging.info(formatted_msg)
    
    # Only print to console for non-debug messages in production
    if log_type != "DEBUG":
        print(formatted_msg)

def get_signer():
    """
    Get OCI signer for authentication.
    """
    try:
        signer = oci.auth.signers.get_resource_principals_signer()
        #signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner() # for local in jenkins
        signer.refresh_security_token()
        log_it("OCI signer initialized successfully", "INFO", "AUTH")
        return signer
    except Exception as e:
        log_it(f"Failed to initialize OCI signer: {str(e)}", "ERROR", "AUTH")
        raise


signer=get_signer()

def get_private_ip(instance_id, compartment_id_instance):
    """
    Get the private IP address of an instance.
    :param instance_id: OCID of the instance
    :param compartment_id_instance: OCID of the compartment containing the instance
    :return: Private IP address of the instance
    """
    try:
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
        return vnic.private_ip
    
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while getting private IP for instance {instance_id}: {str(e)}", "ERROR", "NETWORK")
        raise
    except Exception as e:
        log_it(f"Failed to get private IP for instance {instance_id}: {str(e)}", "ERROR", "NETWORK")
        raise

def is_instance_in_backend(lb_client, lb_id, backend_set_name, private_ip, port):
    """
    Check if an instance is already in the load balancer backend set.
    """
    try:
        port = int(port)  # Convert port to integer
    except (ValueError, TypeError):
        raise ValueError(f"Invalid port value: {port}")
    
    try:
        # Check if the instance's private IP is already a backend
        backends = lb_client.list_backends(lb_id, backend_set_name).data
        for backend in backends:
            if backend.ip_address == private_ip and backend.port == port:
                return True
        return False
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while checking backend for {private_ip}:{port}: {str(e)}", "ERROR", "LOADBALANCER")
        raise
    except Exception as e:
        log_it(f"Failed to check backend for {private_ip}:{port}: {str(e)}", "ERROR", "LOADBALANCER")
        raise

def add_instance_to_lb(lb_id, backend_set_name, instance_id, compartment_id_instance, port):
    """
    Add an instance to the load balancer backend set.
    """
    try:
        port = int(port)  # Convert port to integer
    except (ValueError, TypeError):
        raise ValueError(f"Invalid port value: {port}")
    
    try:
        # Initialize the Load Balancer client
        lb_client = oci.load_balancer.LoadBalancerClient(config={},signer=signer)
        # Get the instance's private IP using the correct compartment ID for the instance
        private_ip = get_private_ip(instance_id, compartment_id_instance)
        # Check if the instance is already added to the backend set
        if is_instance_in_backend(lb_client, lb_id, backend_set_name, private_ip, port):
            log_it(f"Instance {instance_id} (IP: {private_ip}) is already in the backend set", "WARN", "LOADBALANCER")
            return f"[WARN] Instance {instance_id} (IP: {private_ip}) is already in the backend set."
        # Create backend details for the load balancer
        backend_details = oci.load_balancer.models.BackendDetails(
            ip_address=private_ip,
            port=port,
            weight=3  # Default weight
        )
        response = lb_client.create_backend(
            load_balancer_id=lb_id,
            backend_set_name=backend_set_name,
            create_backend_details=backend_details
        )
        if response.status >= 200 and response.status < 300:
            log_it(f"Instance {instance_id} (IP: {private_ip}) added to load balancer successfully", "INFO", "LOADBALANCER")
            return f"[SUCCESS] Instance {instance_id} (IP: {private_ip}) added to load balancer."
        else:
            log_it(f"Instance {instance_id} (IP: {private_ip}) failed to add to load balancer, response={response.status}", "ERROR", "LOADBALANCER")
            return f"[FAILED] Instance {instance_id} (IP: {private_ip}) failed to add to load balancer, response= {response.status}"
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while adding instance {instance_id} to load balancer: {str(e)}", "ERROR", "LOADBALANCER")
        return f"[ERROR] OCI Service Error while adding instance to load balancer: {str(e)}"
    except Exception as e:
        log_it(f"Failed to add backend: {str(e)}", "ERROR", "LOADBALANCER")
        return f"[ERROR] Failed to add backend: {str(e)}"
    

def get_vm_names_and_ids_by_tags(comp_id, freeform_tag_filters={}):
    """Returns VM names and OCIDs for VMs matching the provided tags."""
    try:
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
                    "stage": freeform_tags.get("auto-scale-stage"),
                })
        log_it(f"Found {len(matched)} VMs matching auto-scale tags for stage {freeform_tag_filters.get('auto-scale-stage', 'N/A')}", "INFO", "VM_SEARCH")
        return matched
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while listing instances in compartment {comp_id}: {str(e)}", "ERROR", "VM_SEARCH")
        raise
    except Exception as e:
        log_it(f"Failed to get VMs by tags in compartment {comp_id}: {str(e)}", "ERROR", "VM_SEARCH")
        raise

def start_stop_vm(instance_id, instance_name, action):
    """
    Start, stop, or get status of a VM instance.
    """
    action = action.upper()
    try:
        compute_client = oci.core.ComputeClient(config={}, signer=signer)
        instance = compute_client.get_instance(instance_id).data
        pre_status = instance.lifecycle_state
        if action == "START" and pre_status != "RUNNING":
            compute_client.instance_action(instance_id, "START")
            oci.wait_until(compute_client, compute_client.get_instance(instance_id), 'lifecycle_state', 'RUNNING', max_wait_seconds=120, max_interval_seconds=10)
            # Get updated status after the operation
            updated_instance = compute_client.get_instance(instance_id).data
            status = f"Instance {instance.display_name} started successfully."
        elif action == "STOP" and pre_status != "STOPPED":
            compute_client.instance_action(instance_id, "STOP")
            oci.wait_until(compute_client, compute_client.get_instance(instance_id), 'lifecycle_state', 'STOPPED', max_wait_seconds=120, max_interval_seconds=10)
            # Get updated status after the operation
            updated_instance = compute_client.get_instance(instance_id).data
            status = f"Instance {instance.display_name} stopped successfully."
        elif action == "STATUS":
            updated_instance = instance
            status = f"Instance {instance.display_name} STATUS is {pre_status}"
        else:
            updated_instance = instance
            status = f"Instance {instance.display_name} is already {pre_status.lower()}."
        return {
            "instance_name": instance.display_name,
            "instance_id": instance_id,
            "action": action,
            "pre_status": pre_status,
            "post_status": updated_instance.lifecycle_state,
            "status": status
        }
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while processing VM {instance_id}: {str(e)}", "ERROR", "VM_CONTROL")
        return {
            "instance_name": instance_name,
            "instance_id": instance_id,
            "action": action,
            "error": f"OCI Service Error: {str(e)}"
        }
    except Exception as e:
        log_it(f"Error occurred while processing VM {instance_id}: {str(e)}", "ERROR", "VM_CONTROL")
        return {
            "instance_name": instance_name,
            "instance_id": instance_id,
            "action": action,
            "error": str(e)  # Include the exception message in the error field
        }

def email_message(alarm_payload, environment, stage, action, total_vms, success_count, failure_count, no_op_count, overall_status):
    """
    Generate comprehensive email message body for VM and Load Balancer scaling operations.
    """
    title = alarm_body = alarm_timestamp = ""
    if alarm_payload is not None:
        if "title" in alarm_payload:
            title = alarm_payload["title"]
        if "body" in alarm_payload:
            alarm_body = alarm_payload["body"]
        if "timestampEpochMillis" in alarm_payload:
            time_in_millis = alarm_payload["timestampEpochMillis"] / 1000.0
            alarm_timestamp = datetime.fromtimestamp(time_in_millis).strftime('%Y-%m-%d %H:%M:%S')
    
    # Determine action description and trigger source
    action_desc = "Scale-Up (START)" if action == "START" else "Scale-Down (STOP)"
    trigger_source = title if title else "Manual/Scheduled Operation"
    
    body_msg = f"""Auto Scaling Operation Summary

Environment: {environment} | Stage: {stage} | Action: {action_desc}
Status: {overall_status} | Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Operation Results:
- Total VMs Processed: {total_vms}
- Successful Operations: {success_count}
- Failed Operations: {failure_count}
- No-Operation Required: {no_op_count}

Trigger: {trigger_source}"""

    return body_msg

def send_email(signer, topic_id, email_body=None, subject=""):
    """
    Sends an email to the email notification topic upon completion of the scaling function.
    """
    try:
        if not topic_id:
            log_it("No notification topic ID provided. Skipping email notification.", "WARN", "EMAIL")
            return
        
        ons_client = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)
        message_details = oci.ons.models.MessageDetails(
            body=email_body or "No message body provided",
            title=subject or "Auto Scale Notification")
        publish_message_response = ons_client.publish_message(topic_id, message_details=message_details,
                                                              message_type="RAW_TEXT")
        log_it("Email notification sent successfully", "INFO", "EMAIL")
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while sending email notification: {str(e)}", "ERROR", "EMAIL")
    except Exception as ex:
        log_it(f"Failed to send email notification: {str(ex)}", "ERROR", "EMAIL")

def get_secret(secret_id):
    """
    Retrieves a secret from OCI Vault.
    :param secret_id: OCID of the secret
    :return: The secret content as a string
    """
    try:
        if not secret_id:
            raise ValueError("Secret ID cannot be empty")
        
        secrets_client = oci.secrets.SecretsClient(config={}, signer=signer)
        secret_bundle = secrets_client.get_secret_bundle(secret_id).data
        secret_content = base64.b64decode(secret_bundle.secret_bundle_content.content).decode("utf-8")
        return secret_content
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while retrieving secret {secret_id}: {str(e)}", "ERROR", "SECRETS")
        raise
    except Exception as e:
        log_it(f"Failed to retrieve secret from OCI Vault. Error: {str(e)}", "ERROR", "SECRETS")
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
        # Validate input parameters
        if not all([weblogic_host, server_name, username, password]):
            log_it("Missing required parameters for WebLogic server state check", "ERROR", "WEBLOGIC")
            return False
        
        url = f"https://{weblogic_host}/management/weblogic/latest/domainRuntime/serverLifeCycleRuntimes/{server_name}"
        headers = {
            "Authorization": "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode(),
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers, timeout=10, verify=False)
        if response.status_code == 200:
            server_state = response.json().get("state", "").upper()
            if server_state == "RUNNING":
                log_it(f"WebLogic server {server_name} is RUNNING", "INFO", "WEBLOGIC")
                return True
            else:
                log_it(f"WebLogic server {server_name} is not running. Current state: {server_state}", "WARN", "WEBLOGIC")
                return False
        else:
            log_it(f"Failed to fetch WebLogic server state. HTTP Status: {response.status_code}", "ERROR", "WEBLOGIC")
            return False
    except requests.exceptions.RequestException as e:
        log_it(f"Network error while checking WebLogic server state: {str(e)}", "ERROR", "WEBLOGIC")
        return False
    except Exception as e:
        log_it(f"Exception occurred while checking WebLogic server state: {str(e)}", "ERROR", "WEBLOGIC")
        return False

def check_existing_schedule(resource_scheduler_client, schedule_id):
    """
    Check if a specific schedule exists and is in a valid state for updating.
    Returns True if the schedule exists and can be updated, False otherwise.
    """
    try:
        if not schedule_id:
            log_it("No schedule ID provided. Will create new schedule", "INFO", "SCHEDULER")
            return False
        # Get the specific schedule by ID
        get_schedule_response = resource_scheduler_client.get_schedule(schedule_id=schedule_id)
        schedule = get_schedule_response.data
        # Check if the schedule is in a valid state for updating
        if schedule.lifecycle_state in ["ACTIVE", "INACTIVE"]:
            log_it(f"Found existing schedule {schedule_id} in state {schedule.lifecycle_state}. Can be updated", "INFO", "SCHEDULER")
            return True
        else:
            log_it(f"Schedule {schedule_id} is in state {schedule.lifecycle_state}. Cannot be updated", "WARN", "SCHEDULER")
            return False
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            log_it(f"Schedule {schedule_id} not found. Will create new schedule", "INFO", "SCHEDULER")
        else:
            log_it(f"Failed to get schedule {schedule_id}: {str(e)}", "ERROR", "SCHEDULER")
        return False
    except Exception as e:
        log_it(f"Failed to check schedule {schedule_id}: {str(e)}", "ERROR", "SCHEDULER")
        return False

def schedule_follow_up(auto_scale_env, lb_id, compartment_id, function_id, schedule_id=None):
    """
    Schedule a one-time follow-up function to check the load balancer's health after 15 minutes.
    If schedule_id is provided, updates the existing schedule; otherwise creates a new one.
    """
    try:
        # Initialize the Resource Scheduler client
        resource_scheduler_client = oci.resource_scheduler.ScheduleClient(config={}, signer=signer)
        # Calculate the time 15 minutes from now
        current_time = datetime.utcnow()
        scheduled_time = current_time + timedelta(minutes=15)
        # Check if we should update an existing schedule or create a new one
        if schedule_id and check_existing_schedule(resource_scheduler_client, schedule_id):
            # Update existing schedule
            log_it(f"Updating existing schedule {schedule_id} with new schedule time", "INFO", "SCHEDULER")
            update_schedule_response = resource_scheduler_client.update_schedule(
                schedule_id=schedule_id,
                update_schedule_details=oci.resource_scheduler.models.UpdateScheduleDetails(
                    display_name="Check Load Balancer Health",
                    description="Follow-up to check the health of the load balancer",
                    action="START_RESOURCE",
                    recurrence_details="FREQ=DAILY;COUNT=1",
                    recurrence_type="ICAL",
                    time_starts=scheduled_time.strftime("%Y-%m-%dT%H:%M:%SZ")
                )
            )
            log_it(f"Updated existing schedule {schedule_id} for load balancer health check", "INFO", "SCHEDULER")
        else:
            # Create new schedule
            log_it("Creating new schedule for load balancer health check", "INFO", "SCHEDULER")
            schedule_details = oci.resource_scheduler.models.CreateScheduleDetails(
                compartment_id=compartment_id,
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
            log_it("Created new schedule for load balancer health check", "INFO", "SCHEDULER")
            
    except Exception as e:
        log_it(f"Failed to schedule follow-up job. Error: {str(e)}", "ERROR", "SCHEDULER")

def drain_backend(lb_client, lb_id, backend_set_name, private_ip, port, timeout=240, interval=5):
    """
    Drains traffic from the backend by setting its weight to 1 and waits until it is drained.
    """
    log_it(f"Draining backend {private_ip}:{port} in backend set {backend_set_name}", "INFO", "LOADBALANCER")
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
        log_it(f"Drain initiated for backend {private_ip}:{port}. Checking status...", "INFO", "LOADBALANCER")
        # Poll for drain status
        elapsed_time = 0
        while elapsed_time < timeout:
            backends = lb_client.list_backends(lb_id, backend_set_name).data
            for backend in backends:
                if backend.ip_address == private_ip and backend.port == port:
                    if not backend.drain:  # Check if the backend is still draining
                        log_it(f"Backend {private_ip}:{port} is still draining. Waiting...", "INFO", "LOADBALANCER")
                        time.sleep(interval)
                        elapsed_time += interval
                        break
                    else:
                        log_it(f"Backend {private_ip}:{port} drained successfully", "INFO", "LOADBALANCER")
                        return True
            else:
                log_it(f"Backend {private_ip}:{port} not found in backend set {backend_set_name}", "ERROR", "LOADBALANCER")
                return False
        log_it(f"Timeout reached while draining backend {private_ip}:{port}", "ERROR", "LOADBALANCER")
        return False
    except Exception as e:
        log_it(f"Failed to drain backend {private_ip}:{port}. Error: {str(e)}", "ERROR", "LOADBALANCER")
        return False

def mark_backend_offline(lb_client, lb_id, backend_set_name, private_ip, port, timeout=240, interval=5):
    """
    Marks the backend as offline and waits until it is fully offline, but only if it is already draining.
    """
    log_it(f"Marking backend {private_ip}:{port} as offline in backend set {backend_set_name}", "INFO", "LOADBALANCER")
    try:
        # Check if the backend is already draining
        backends = lb_client.list_backends(lb_id, backend_set_name).data
        for backend in backends:
            if backend.ip_address == private_ip and backend.port == port:
                if not backend.drain:  # Backend is not draining
                    log_it(f"Backend {private_ip}:{port} is not draining. Skipping offline operation", "ERROR", "LOADBALANCER")
                    return False
                if backend.offline:  # Backend is already offline
                    log_it(f"Backend {private_ip}:{port} is already offline. Skipping offline operation", "INFO", "LOADBALANCER")
                    return True
                break
        else:
            log_it(f"Backend {private_ip}:{port} not found in backend set {backend_set_name}", "ERROR", "LOADBALANCER")
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
        log_it(f"Offline operation initiated for backend {private_ip}:{port}. Checking status...", "INFO", "LOADBALANCER")
        # Poll for offline status
        elapsed_time = 0
        while elapsed_time < timeout:
            backends = lb_client.list_backends(lb_id, backend_set_name).data
            for backend in backends:
                if backend.ip_address == private_ip and backend.port == port:
                    if not backend.offline:  # Backend is not yet offline
                        log_it(f"Backend {private_ip}:{port} is still online. Waiting...", "INFO", "LOADBALANCER")
                        time.sleep(interval)
                        elapsed_time += interval
                        break
                    else:  # Backend is fully offline
                        log_it(f"Backend {private_ip}:{port} marked as offline successfully", "INFO", "LOADBALANCER")
                        return True
            else:
                log_it(f"Backend {private_ip}:{port} not found in backend set {backend_set_name}", "ERROR", "LOADBALANCER")
                return False
        log_it(f"Timeout reached while marking backend {private_ip}:{port} as offline", "ERROR", "LOADBALANCER")
        return False
    except Exception as e:
        log_it(f"Failed to mark backend offline {private_ip}:{port}. Error: {str(e)}", "ERROR", "LOADBALANCER")
        return False

def remove_backend(lb_client, lb_id, backend_set_name, private_ip, port, timeout=240, interval=5):
    """
    Removes the backend from the backend set only if it is already offline.
    """
    log_it(f"Removing backend {private_ip}:{port} from backend set {backend_set_name}", "INFO", "LOADBALANCER")
    try:
        # Check if the backend is offline
        backends = lb_client.list_backends(lb_id, backend_set_name).data
        for backend in backends:
            if backend.ip_address == private_ip and backend.port == port:
                if not backend.offline:  # Backend is not offline
                    log_it(f"Backend {private_ip}:{port} is not offline. Skipping removal", "ERROR", "LOADBALANCER")
                    return False
                break
        else:
            log_it(f"Backend {private_ip}:{port} not found in backend set {backend_set_name}", "ERROR", "LOADBALANCER")
            return False
        # Initiate the remove operation
        lb_client.delete_backend(
            load_balancer_id=lb_id,
            backend_set_name=backend_set_name,
            backend_name=f"{private_ip}:{port}"
        )
        log_it(f"Remove operation initiated for backend {private_ip}:{port}. Checking status...", "INFO", "LOADBALANCER")
        # Poll for removal status
        elapsed_time = 0
        while elapsed_time < timeout:
            backends = lb_client.list_backends(lb_id, backend_set_name).data
            backend_exists = False
            for backend in backends:
                if backend.ip_address == private_ip and backend.port == port:
                    backend_exists = True
                    log_it(f"Backend {private_ip}:{port} still exists. Waiting...", "INFO", "LOADBALANCER")
                    time.sleep(interval)
                    elapsed_time += interval
                    break
            if not backend_exists:  # Backend has been successfully removed
                log_it(f"Backend {private_ip}:{port} removed successfully", "INFO", "LOADBALANCER")
                return True
        log_it(f"Timeout reached while removing backend {private_ip}:{port}", "ERROR", "LOADBALANCER")
        return False
    except Exception as e:
        log_it(f"Failed to remove backend {private_ip}:{port}. Error: {str(e)}", "ERROR", "LOADBALANCER")
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
                log_it(f"Command executed successfully on VM {instance_id}", "INFO", "VM_COMMAND")
                return True
            elif command_status in ["FAILED", "CANCELED"]:
                log_it(f"Command execution failed on VM {instance_id}. Status: {command_status}", "ERROR", "VM_COMMAND")
                return False
            time.sleep(interval)
            elapsed_time += interval
        log_it(f"Command execution timed out on VM {instance_id}", "ERROR", "VM_COMMAND")
        return False
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while executing command on VM {instance_id}. Error: {str(e)}", "ERROR", "VM_COMMAND")
        return False
    except Exception as e:
        log_it(f"Failed to execute command on VM {instance_id}. Error: {str(e)}", "ERROR", "VM_COMMAND")
        return False

def scale_down_vm(vm, compartment_id):
    """
    Handles the scale-down process for a single VM.
    Optimized to combine service stop commands and reduce execution time.
    """
    try:
        # Validate VM properties
        required_vm_props = ['ocid', 'name', 'lb_ocid', 'backend', 'port']
        missing_props = [prop for prop in required_vm_props if not vm.get(prop)]
        if missing_props:
            error_msg = f"VM missing required properties: {', '.join(missing_props)}"
            return {"vm_name": vm.get('name', 'Unknown'), "status": "failure", "reason": error_msg}
        
        lb_client = oci.load_balancer.LoadBalancerClient(config={}, signer=signer)
        compute_client = oci.core.ComputeClient(config={}, signer=signer)
        # Check VM state
        instance = compute_client.get_instance(vm['ocid']).data
        if instance.lifecycle_state == "STOPPED":
            log_it(f"VM {vm['name']} is already in STOPPED state. Skipping scale-down", "INFO", "VM_CONTROL")
            return {"vm_name": vm['name'], "status": "no-op", "reason": "VM already stopped"}
        # Check if VM is in the load balancer
        private_ip = get_private_ip(vm['ocid'], compartment_id)
        
        if not is_instance_in_backend(lb_client, vm['lb_ocid'], vm['backend'], private_ip, int(vm['port'])):
            log_it(f"VM {vm['name']} is not part of the load balancer. Skipping scale-down", "INFO", "VM_CONTROL")
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
        return {"vm_name": vm.get('name', 'Unknown'), "status": "failure", "reason": str(e)}
 
def log_summary_to_nosql(nosql_client, table_name, table_compartment_id, action, environment, stage, total_vms, success_count, failure_count, no_op_count, overall_status):
    """
    Logs a summary of the scale action into the NoSQL table.
    """
    try:
        if not all([nosql_client, table_name, table_compartment_id]):
            log_it("Missing required parameters for NoSQL logging", "ERROR", "NOSQL")
            return
        
        log_entry = {
            "Action": action,
            "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
            "Environment": environment,
            "Stage": stage,
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
        log_it(f"Logged summary for action {action} in environment {environment}. Overall Status: {overall_status}", "INFO", "NOSQL")
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while logging to NoSQL: {str(e)}", "ERROR", "NOSQL")
    except Exception as e:
        log_it(f"Failed to log summary for action {action}. Error: {str(e)}", "ERROR", "NOSQL")

def get_last_scale_action(nosql_client, table_name, environment, stage, compartment_id):
    """
    Queries the NoSQL table for all scale actions for the given environment and stage,
    then returns the action from the record with the latest Timestamp.
    Returns Action, Timestamp, and Overall_Status for enhanced state validation.
    """
    try:
        if not all([nosql_client, table_name, environment, stage, compartment_id]):
            log_it("Missing required parameters for NoSQL query", "ERROR", "NOSQL")
            return None
        
        query_statement = (
            f"SELECT Action, Timestamp, Overall_Status FROM {table_name} "
            f"WHERE Environment = '{environment}' AND Stage = '{stage}'"
        )
        query_response = nosql_client.query(
            query_details=oci.nosql.models.QueryDetails(
                compartment_id=compartment_id, 
                statement=query_statement
            )
        )
        rows = query_response.data.items
        if not rows:
            log_it(f"No previous scale action found for environment {environment}, stage {stage}", "INFO", "NOSQL")
            return None
        rows_sorted = sorted(rows, key=lambda x: x.get("Timestamp", ""), reverse=True)
        log_it(f"Last scale action for environment {environment}, stage {stage} retrieved successfully", "INFO", "NOSQL")
        log_it(f"Last Sorted Query response: {rows_sorted[0]}", "DEBUG", "NOSQL")
        return rows_sorted[0]
    except oci.exceptions.ServiceError as e:
        log_it(f"OCI Service Error while querying NoSQL: {str(e)}", "ERROR", "NOSQL")
        return None
    except Exception as e:
        log_it(f"Failed to query last scale action: {str(e)}", "ERROR", "NOSQL")
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
        try:
            body = json.loads(data.getvalue())
            parsed_body = json.loads(body.get("body", "{}"))
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON input: {str(e)}")
        
        auto_scale_env = parsed_body.get("auto_scale_env")
        action = parsed_body.get("action")
        
        # Validate required input parameters
        if not auto_scale_env:
            raise ValueError("auto_scale_env is required")
        if not action:
            raise ValueError("action is required")
            
        action = action.upper()  # Convert action to uppercase
        
        # Validate and get required environment variables
        required_env_vars = {
            "KEY_COMPARTMENT_OCID": os.environ.get("KEY_COMPARTMENT_OCID"),
            "TABLE_COMPARTMENT_OCID": os.environ.get("TABLE_COMPARTMENT_OCID"),
            "TABLE_NAME": os.environ.get("TABLE_NAME"),
            "WEBLOGIC_HOST": os.environ.get("WEBLOGIC_HOST"),
            "WEBLOGIC_USERNAME": os.environ.get("WEBLOGIC_USERNAME"),
            "WEBLOGIC_PASSWORD_SECRET_OCID": os.environ.get("WEBLOGIC_PASSWORD_SECRET_OCID"),
            "ADMIN_SERVER_NAME": os.environ.get("ADMIN_SERVER_NAME")
        }
        
        missing_vars = [var for var, value in required_env_vars.items() if not value]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
        
        compartment_id = required_env_vars["KEY_COMPARTMENT_OCID"]
        table_compartment_id = required_env_vars["TABLE_COMPARTMENT_OCID"]
        table_name = required_env_vars["TABLE_NAME"]
        weblogic_host = required_env_vars["WEBLOGIC_HOST"]
        username = required_env_vars["WEBLOGIC_USERNAME"]
        password_secret_id = required_env_vars["WEBLOGIC_PASSWORD_SECRET_OCID"]
        admin_server_name = required_env_vars["ADMIN_SERVER_NAME"]
        
        # Optional environment variables
        function_id = os.environ.get("CHECK_LOAD_BALANCER_HEALTH_OCID")
        schedule_id = os.environ.get("HEALTH_CHECK_SCHEDULE_OCID")  # Optional: Existing schedule ID to update
        stage = str(parsed_body.get("auto-scale-stage", "1"))  # Ensure stage is always a string

        password = get_secret(password_secret_id)

        if (action == "START"):
            # Check WebLogic Admin Server state
            if not check_weblogic_server_state(weblogic_host, admin_server_name, username, password):
                logs.append("[ERROR] WebLogic Admin Server is not running. Aborting operations.")
                return response.Response(ctx, response_data=json.dumps({"error": "WebLogic Admin Server is not running", "logs": logs}), headers={"Content-Type": "application/json"})

        logs.append(f"auto_scale_env={auto_scale_env}, action={action}, stage={stage}")
        log_it(f"Processing scale operation: env={auto_scale_env}, action={action}, stage={stage}", "INFO", "HANDLER")

        # Define tag filters
        freeform_tag_filters = {
            "auto-scale": "enabled",
            "auto-scale-env": auto_scale_env,
            "auto-scale-stage": str(stage),
            "auto-scale-backend": "*",
            "auto-scale-lb-ocid": "*",
            "auto-scale-port": "*"
        }

        # Initialize NoSQL client for checking last action status
        nosql_client = oci.nosql.NosqlClient(config={}, signer=signer)
        
        # Check if last action for this stage and environment resulted in "No Operation"
        # If so, skip execution as desired state is already achieved
        last_record = get_last_scale_action(nosql_client, table_name, auto_scale_env, stage, table_compartment_id)
        if last_record:
            last_action = last_record.get("Action")
            last_status = last_record.get("Overall_Status") 
            last_timestamp = last_record.get("Timestamp")
            
            # If last action was the same as current action and resulted in "Success" or "No Operation"
            # then desired state is already achieved - skip execution
            if last_action == action and last_status in ["Success", "No Operation"]:
                if last_status == "No Operation":
                    skip_reason = f"Last {action} action for Stage {stage} resulted in 'No Operation' status. Desired state already achieved."
                else:
                    skip_reason = f"Last {action} action for Stage {stage} was successful. VMs are already in desired state."
                logs.append(f"[INFO] {skip_reason}")
                log_it(skip_reason, "INFO", "OPTIMIZATION")
                
                # Return early with no-op response
                return response.Response(ctx, response_data=json.dumps({
                    "logs": logs, 
                    "output": [f"Skipped execution: {skip_reason}"],
                    "optimization": "no_operation_skip"
                }), headers={"Content-Type": "application/json"})

        # Fetch VMs matching the tags
        vm_list = get_vm_names_and_ids_by_tags(compartment_id, freeform_tag_filters)
        log_it(f"Found {len(vm_list)} VMs for processing", "INFO", "HANDLER")
        
        if not vm_list:
            logs.append(f"[INFO] No VMs found matching the auto-scale tags for environment {auto_scale_env}, stage {stage}")
            return response.Response(ctx, response_data=json.dumps({"logs": logs, "output": []}), headers={"Content-Type": "application/json"})

        if action == "STOP":
            # Initialize NoSQL client and table variables if not already done
            nosql_client = oci.nosql.NosqlClient(config={}, signer=signer)
            
            # Check stage dependency for STOP: Higher stages must be stopped before lower stages
            # For example, Stage 1 can only be stopped if Stage 2 has already been stopped
            if int(stage) == 1:  # Stage 1 is the base/core stage
                # Check if any higher stages are still running
                higher_stages_running = []
                max_stage_to_check = int(os.environ.get("MAX_SCALE_STAGES", "2"))  # Configurable max stages
                
                for check_stage in range(2, max_stage_to_check + 1):
                    higher_stage_record = get_last_scale_action(nosql_client, table_name, auto_scale_env, str(check_stage), table_compartment_id)
                    # Only consider as "running" if the last action was START (regardless of success level)
                    if (higher_stage_record and higher_stage_record.get("Action") == "START"):
                        higher_stages_running.append(str(check_stage))
                
                if higher_stages_running:
                    logs.append(f"[ERROR] Stage {stage} cannot be stopped. Higher stages {higher_stages_running} have been started and should be stopped first.")
                    log_it(f"Stage {stage} cannot be stopped. Higher stages {higher_stages_running} should be stopped first", "ERROR", "STAGE_VALIDATION")
                    return response.Response(ctx, response_data=json.dumps({
                        "error": f"Stage dependency not met. Stages {higher_stages_running} should be stopped before Stage {stage}",
                        "logs": logs, 
                        "output": []
                    }), headers={"Content-Type": "application/json"})

                logs.append(f"[INFO] Stage dependency validated for STOP. No higher stages need to be stopped first. Proceeding with Stage {stage} shutdown.")
                log_it(f"Stage dependency validated for STOP. Proceeding with Stage {stage} shutdown", "INFO", "STAGE_VALIDATION")

            # Check minimum time gap between START and STOP (only if there was a recent START)
            last_record = get_last_scale_action(nosql_client, table_name, auto_scale_env, stage, table_compartment_id)
            if last_record and last_record.get("Action") == "START":
                last_timestamp = last_record.get("Timestamp")
                try:
                    # Try parsing with fractional seconds; adjust format as needed
                    parsed_ts = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    parsed_ts = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                
                min_runtime_hours = int(os.environ.get("MIN_STAGE_RUNTIME_HOURS", "1"))  # Configurable minimum runtime (default: 1 hour)
                if (datetime.utcnow() - parsed_ts) < timedelta(hours=min_runtime_hours):
                    log_it(f"Stage {stage} START action was performed within the last {min_runtime_hours} hour(s). Skipping STOP to allow sufficient runtime", "INFO", "STAGE_VALIDATION")
                    logs.append(f"[INFO] Stage {stage} START action was performed within the last {min_runtime_hours} hour(s). Skipping STOP to allow sufficient runtime.")
                    return response.Response(ctx, response_data=json.dumps({"logs": logs, "output": []}), headers={"Content-Type": "application/json"})
            
            # Proceed with STOP operations
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = executor.map(lambda vm: scale_down_vm(vm, compartment_id), vm_list)
                for vm_action_result in results:
                    output.append(vm_action_result)
                    if vm_action_result["status"] == "success":
                        logs.append(f"[INFO] VM {vm_action_result['vm_name']} scaled down successfully.")
                        success_vms_state.append(vm_action_result["vm_name"])
                    elif vm_action_result["status"] == "no-op":
                        logs.append(f"[INFO] VM {vm_action_result['vm_name']} is already in desired state. Reason: {vm_action_result['reason']}")
                        if "already stopped" in vm_action_result["reason"].lower():
                            no_op_vms["already_stopped"].append(f"{vm_action_result['vm_name']} (Status: STOPPED)")
                        else:
                            no_op_vms["already_stopped"].append(f"{vm_action_result['vm_name']} ({vm_action_result['reason']})")
                    else:
                        logs.append(f"[WARN] Failed to scale down VM {vm_action_result['vm_name']}. Reason: {vm_action_result['reason']}")
                        failed_vms.append({"vm_name": vm_action_result["vm_name"], "reason": vm_action_result["reason"]})
                        
        elif action == "START":
            # Initialize NoSQL client and table variables for START actions
            nosql_client = oci.nosql.NosqlClient(config={}, signer=signer)
            
            # Check stage dependency: Stage 2+ can only run if previous stage was started
            if int(stage) > 1:
                previous_stage = str(int(stage) - 1)
                previous_stage_record = get_last_scale_action(nosql_client, table_name, auto_scale_env, previous_stage, table_compartment_id)
                
                if not previous_stage_record or previous_stage_record.get("Action") != "START":
                    logs.append(f"[ERROR] Stage {stage} cannot be triggered. Previous stage {previous_stage} has not been started yet.")
                    log_it(f"Stage {stage} cannot be triggered. Previous stage {previous_stage} has not been started yet", "ERROR", "STAGE_VALIDATION")
                    return response.Response(ctx, response_data=json.dumps({
                        "error": f"Stage dependency not met. Stage {previous_stage} must be started before Stage {stage}",
                        "logs": logs, 
                        "output": []
                    }), headers={"Content-Type": "application/json"})

                logs.append(f"[INFO] Stage dependency validated. Previous stage {previous_stage} was started. Proceeding with Stage {stage}.")
                log_it(f"Stage dependency validated. Previous stage {previous_stage} was started. Proceeding with Stage {stage}", "INFO", "STAGE_VALIDATION")
            
            # Check for recent START operations for the current stage to prevent concurrent operations
            last_record = get_last_scale_action(nosql_client, table_name, auto_scale_env, stage, table_compartment_id)
            last_action = last_record.get("Action") if last_record else None
            last_timestamp = last_record.get("Timestamp") if last_record else None
            
            if last_action == "START" and last_timestamp:
                try:
                    # Try parsing with fractional seconds; adjust format as needed
                    parsed_ts = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                except ValueError:
                    parsed_ts = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                
                # Check if START was performed within the last hour to prevent concurrent operations
                concurrent_prevention_hours = float(os.environ.get("CONCURRENT_PREVENTION_HOURS", "1"))  # Configurable prevention window
                if (datetime.utcnow() - parsed_ts) < timedelta(hours=concurrent_prevention_hours):
                    logs.append(f"[INFO] Recent START action for Stage {stage} was performed within the last {concurrent_prevention_hours} hour(s). Skipping to avoid concurrent operations.")
                    log_it(f"Recent START action for Stage {stage} was performed within the last {concurrent_prevention_hours} hour(s). Skipping to avoid concurrent operations", "INFO", "STAGE_VALIDATION")
                    return response.Response(ctx, response_data=json.dumps({"logs": logs, "output": []}), headers={"Content-Type": "application/json"})
            
            # Process each VM for scale-up if no recent START action was recorded
            for vm in vm_list:
                try:
                    # Validate VM properties
                    required_vm_props = ['ocid', 'name', 'lb_ocid', 'backend', 'port']
                    missing_props = [prop for prop in required_vm_props if not vm.get(prop)]
                    if missing_props:
                        error_msg = f"VM {vm.get('name', 'Unknown')} missing required properties: {', '.join(missing_props)}"
                        failed_vms.append({"vm_name": vm.get('name', 'Unknown'), "reason": error_msg})
                        logs.append(f"[ERROR] {error_msg}")
                        continue
                    
                    # Perform the start operation
                    vm_action_result = start_stop_vm(vm['ocid'], vm['name'], action)
                    output.append(vm_action_result)
                    
                    pre_status = vm_action_result.get("pre_status", "").upper()
                    post_status = vm_action_result.get("post_status", "").upper()
                    status_message = vm_action_result.get("status", "").lower()
                    error_message = vm_action_result.get("error")
                    
                    vm_needs_lb_operation = False
                    
                    if error_message:
                        # VM operation failed due to error
                        failed_vms.append({"vm_name": vm['name'], "reason": error_message})
                        logs.append(f"[WARN] Failed to start VM {vm['name']}. Error: {error_message}")
                        continue  # Skip adding to the load balancer if start fails
                    elif action == "START" and pre_status == "RUNNING":
                        # VM was already running - still need to check LB status
                        logs.append(f"[INFO] VM {vm['name']} is already running. Checking Load Balancer status.")
                        no_op_vms["already_running"].append(f"{vm['name']} (Status: {post_status})")
                        vm_needs_lb_operation = True
                    elif "successfully" in status_message:
                        # VM was started successfully - definitely needs LB operation
                        logs.append(f"[INFO] VM {vm['name']} started successfully.")
                        success_vms_state.append(vm['name'])
                        vm_needs_lb_operation = True
                    elif "already" in status_message and "running" in status_message:
                        # Additional check for "already running" message from start_stop_vm
                        logs.append(f"[INFO] VM {vm['name']} is already in desired state. Checking Load Balancer status.")
                        no_op_vms["already_running"].append(f"{vm['name']} (Status: {post_status})")
                        vm_needs_lb_operation = True
                    else:
                        # Unexpected status - treat as failure
                        failed_vms.append({"vm_name": vm['name'], "reason": f"Unexpected status: {status_message}"})
                        logs.append(f"[WARN] Failed to start VM {vm['name']}. Unexpected status: {status_message}")
                        continue  # Skip adding to the load balancer if start fails
                        
                    # Add instance to Load Balancer only if VM is running (successfully started or already running)
                    if vm_needs_lb_operation:
                        out_lb_add = add_instance_to_lb(vm['lb_ocid'], vm['backend'], vm['ocid'], compartment_id, vm['port'])
                        output.append(out_lb_add)
                        if "success" in out_lb_add.lower():
                            logs.append(f"[INFO] VM {vm['name']} added to Load Balancer successfully.")
                            success_vms_lb.append(f"{vm['name']} (LB: {vm['backend']}, Port: {vm['port']})")
                        elif "already in the backend set" in out_lb_add.lower():
                            logs.append(f"[INFO] VM {vm['name']} is already part of the Load Balancer.")
                            no_op_lb.append(f"{vm['name']} (LB: {vm['backend']}, Port: {vm['port']})")
                        else:
                            # LB operation failed - this should be treated as a partial failure
                            failed_vms.append({"vm_name": vm['name'], "reason": f"VM started but LB operation failed: {out_lb_add}"})
                            logs.append(f"[WARN] VM {vm['name']} started successfully but failed to add to Load Balancer: {out_lb_add}")
                except Exception as vm_error:
                    failed_vms.append({"vm_name": vm['name'], "reason": str(vm_error)})
                    logs.append(f"[ERROR] Failed to process VM {vm['name']}. Error: {str(vm_error)}")

        # Schedule follow-up function
        if success_vms_lb and vm_list and vm_list[0].get('lb_ocid'):
            schedule_follow_up(auto_scale_env, vm_list[0]['lb_ocid'], compartment_id, function_id, schedule_id)

        # After processing all VMs
        total_vms = len(vm_list)
        success_count = len(success_vms_state)
        no_op_count = len(no_op_vms["already_running"]) + len(no_op_vms["already_stopped"]) + len(no_op_lb)
        
        # Get names of VMs that are in no-op state
        no_op_vm_names = set()
        no_op_vm_names.update([vm.split(" (")[0] for vm in no_op_vms["already_running"]])  # Extract VM name from "VM (Status: ...)"
        no_op_vm_names.update([vm.split(" (")[0] for vm in no_op_vms["already_stopped"]])  # Extract VM name from "VM (Status: ...)"
        no_op_vm_names.update([vm.split(" (")[0] for vm in no_op_lb])  # Extract VM name from "VM (LB: ...)"
        
        # Count only actual failures (exclude no-op VMs from failed_vms)
        actual_failures = [vm for vm in failed_vms if vm["vm_name"] not in no_op_vm_names]
        failure_count = len(actual_failures)

        # Enhanced overall status determination with better logic
        effective_no_op_count = no_op_count
        effective_failure_count = failure_count
        
        # For START actions, consider load balancer results in overall status
        if action == "START":
            # If all VMs are already running AND already in LB, it's truly "No Operation"
            vm_count = len(no_op_vms["already_running"])
            lb_count = len(no_op_lb)
            
            # Determine overall status for START action
            if effective_failure_count == 0 and success_count == 0 and vm_count > 0 and lb_count > 0 and vm_count == lb_count == total_vms:
                # Perfect no-op: all VMs already running and already in LB
                overall_status = "No Operation"
            elif effective_failure_count == 0 and (success_count > 0 or len(success_vms_lb) > 0):
                # Some actual work was done successfully
                overall_status = "Success"
            elif effective_failure_count > 0 and (success_count > 0 or len(success_vms_lb) > 0 or effective_no_op_count > 0):
                # Mixed results
                overall_status = "Partial Success"
            elif effective_failure_count == total_vms:
                # All operations failed
                overall_status = "Failure"
            elif effective_failure_count == 0 and success_count == 0 and effective_no_op_count > 0:
                # Some no-ops but not perfect alignment (e.g., VM running but not in LB)
                overall_status = "No Operation"
            else:
                overall_status = "Failure"  # Fallback
        else:
            # For STOP actions, use simpler logic
            if effective_no_op_count == total_vms and effective_failure_count == 0:
                overall_status = "No Operation"
            elif effective_failure_count == 0 and success_count > 0:
                overall_status = "Success"
            elif effective_failure_count > 0 and (success_count > 0 or effective_no_op_count > 0):
                overall_status = "Partial Success"
            elif effective_failure_count == total_vms:
                overall_status = "Failure"
            else:
                overall_status = "No Operation" if effective_no_op_count > 0 else "Failure"

        # Enhanced logging with detailed metrics
        operation_metrics = {
            "total_vms": total_vms,
            "vm_successes": success_count,
            "vm_failures": len([vm for vm in failed_vms if vm["vm_name"] not in no_op_vm_names]),
            "vm_no_ops": len(no_op_vms["already_running"]) + len(no_op_vms["already_stopped"]),
            "lb_successes": len(success_vms_lb),
            "lb_no_ops": len(no_op_lb),
            "lb_failures": len([vm for vm in failed_vms if "LB operation failed" in vm.get("reason", "")]),
            "overall_status": overall_status,
            "optimization_applied": False
        }
        
        log_it(f"Operation completed - Metrics: {operation_metrics}", "INFO", "METRICS")
        
        # Log ALL operations to NoSQL for complete audit trail
        # This includes successes, failures, and no-operations for tracking and debugging
        log_summary_to_nosql(
            nosql_client,
            table_name,
            table_compartment_id,
            action=action,
            environment=auto_scale_env,
            stage=stage,
            total_vms=total_vms,
            success_count=success_count,
            failure_count=failure_count,
            no_op_count=no_op_count,
            overall_status=overall_status
        )
        log_it(f"NoSQL operation logged for {action} with status: {overall_status}", "INFO", "NOSQL")

        # Construct email content
        notification_topic_id = os.environ.get("wlsc_email_notification_topic_id")
        if notification_topic_id:
            # Only send email if there are actual operations performed or failures occurred
            # Skip email for cases where function was optimized/skipped entirely
            if success_vms_state or success_vms_lb or failed_vms:
                subject = f"Auto Scale {action} - Stage {stage} - {auto_scale_env} - {overall_status}"
                
                # Generate enhanced email content with better formatting
                email_body = email_message(
                    alarm_payload=body,
                    environment=auto_scale_env,
                    stage=stage,
                    action=action,
                    total_vms=total_vms,
                    success_count=success_count,
                    failure_count=failure_count,
                    no_op_count=no_op_count,
                    overall_status=overall_status
                )
                
                # Add execution summary
                email_body += f"\n\n=== EXECUTION SUMMARY ==="
                email_body += f"\nVM Operations: {success_count} successful, {failure_count} failed, {len(no_op_vms['already_running']) + len(no_op_vms['already_stopped'])} no-op"
                if action == "START":
                    email_body += f"\nLoad Balancer Operations: {len(success_vms_lb)} successful, {len(no_op_lb)} no-op"
                
                # Add detailed operation breakdown
                if success_vms_state or success_vms_lb:
                    email_body += "\n\n=== SUCCESSFUL OPERATIONS ==="
                    if success_vms_state:
                        email_body += f"\nVM State Changes ({len(success_vms_state)}):"
                        for i, vm_name in enumerate(success_vms_state, 1):
                            email_body += f"\n  {i}. {vm_name} - {action}ED successfully"
                    
                    if success_vms_lb:
                        email_body += f"\nLoad Balancer Operations ({len(success_vms_lb)}):"
                        for i, vm_info in enumerate(success_vms_lb, 1):
                            email_body += f"\n  {i}. {vm_info} - Added to Load Balancer"
                
                # Include no-op details only when there are also actual operations (for context)
                if no_op_vms["already_running"] or no_op_vms["already_stopped"] or no_op_lb:
                    email_body += "\n\n=== NO-OPERATION (ALREADY IN DESIRED STATE) ==="
                    if no_op_vms["already_running"]:
                        email_body += f"\nVMs Already Running ({len(no_op_vms['already_running'])}):"
                        for i, vm_info in enumerate(no_op_vms["already_running"], 1):
                            email_body += f"\n  {i}. {vm_info}"
                    
                    if no_op_vms["already_stopped"]:
                        email_body += f"\nVMs Already Stopped ({len(no_op_vms['already_stopped'])}):"
                        for i, vm_info in enumerate(no_op_vms["already_stopped"], 1):
                            email_body += f"\n  {i}. {vm_info}"
                    
                    if no_op_lb:
                        email_body += f"\nVMs Already in Load Balancer ({len(no_op_lb)}):"
                        for i, vm_info in enumerate(no_op_lb, 1):
                            email_body += f"\n  {i}. {vm_info}"
                
                if failed_vms:
                    email_body += "\n\n=== FAILED OPERATIONS ==="
                    email_body += f"\nFailed Operations ({len(failed_vms)}):"
                    for i, failure in enumerate(failed_vms, 1):
                        vm_name = failure.get('vm_name', 'Unknown')
                        reason = failure.get('reason', 'No specific reason provided')
                        # Truncate very long error messages for email readability
                        if len(reason) > 200:
                            reason = reason[:200] + "... (truncated)"
                        email_body += f"\n  {i}. {vm_name}: {reason}"
                    
                    email_body += "\n\nACTION REQUIRED:"
                    email_body += "\n• Check Oracle Functions logs for detailed error information"
                    email_body += "\n• Verify VM and Load Balancer accessibility"
                    email_body += "\n• Consider manual intervention if errors persist"
                
                send_email(
                    signer=signer,
                    topic_id=notification_topic_id,
                    email_body=email_body,
                    subject=subject
                )

        return response.Response(ctx, response_data=json.dumps({"output": output, "logs": logs}), headers={"Content-Type": "application/json"})

    except Exception as e:
        error_msg = f"Unexpected error in Stage {stage if 'stage' in locals() else 'Unknown'} {action if 'action' in locals() else 'operation'}: {str(e)}"
        log_it(error_msg, "ERROR", "HANDLER")
        logs.append(f"[ERROR] {error_msg}")
        
        notification_topic_id = os.environ.get("wlsc_email_notification_topic_id")
        if notification_topic_id:
            env_info = auto_scale_env if 'auto_scale_env' in locals() else 'Unknown'
            stage_info = stage if 'stage' in locals() else 'Unknown'
            action_info = action if 'action' in locals() else 'Unknown'
            
            email_body = f"""Auto Scaling Error Notification

Environment: {env_info} | Stage: {stage_info} | Action: {action_info}
Error Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

=== ERROR DETAILS ===
{str(e)}

=== ACTION REQUIRED ===
Check Oracle Functions logs for detailed error traces and verify system configuration.
Review environment variables, NoSQL connectivity, and VM/Load Balancer accessibility."""
            
            send_email(
                signer=oci.auth.signers.get_resource_principals_signer(),
                topic_id=notification_topic_id,
                email_body=email_body,
                subject=f"Auto Scale ERROR - Stage {stage_info} - {env_info} - {action_info}"
            )
        return response.Response(ctx, response_data=json.dumps({"error": str(e), "logs": logs}), headers={"Content-Type": "application/json"})