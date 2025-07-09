import io
import json
import logging
import os
import oci
from fdk import response


def handler(ctx, data: io.BytesIO = None):
    """
    Entry point for the OCI Function. Performs the load balancer health check and sends an email with the health report.
    """
    logs = []
    try:
        # Parse input data
        body = json.loads(data.getvalue())
        auto_scale_env = body.get("auto_scale_env")
        lb_id = body.get("lb_id")
        vm_compartment_id = body.get("compartment_id")

        # Initialize the Load Balancer client
        signer = oci.auth.signers.get_resource_principals_signer()
        lb_client = oci.load_balancer.LoadBalancerClient(config={}, signer=signer)

        # Fetch the load balancer health
        health = lb_client.get_load_balancer_health(lb_id).data
        logs.append(f"[INFO] Load Balancer Health Status: {health.status}")

        # Generate the health report
        health_report = f"Load Balancer Health Report for {auto_scale_env}:\n"
        health_report += f"Load Balancer Health Status: {health.status}\n\n"
        # Fetch the load balancer details for backend information
        load_balancer = lb_client.get_load_balancer(lb_id).data
        logs.append(f"[INFO] Load Balancer Retrieved: {load_balancer.display_name}")
        health_report += f"Load Balancer Name: {load_balancer.display_name}\n\n"

# Log backend health details
        if hasattr(load_balancer, 'backend_sets') and load_balancer.backend_sets:
            for backend_set_name, backend_set in load_balancer.backend_sets.items():
                health_report += f"Backend Set: {backend_set_name}, Policy: {backend_set.policy}\n"
                for backend in backend_set.backends:
                    # Fetch the VM display name using the backend IP address
                    vm_display_name = get_vm_display_name_by_ip(backend.ip_address, vm_compartment_id, signer)
                    vm_info = f"({vm_display_name})" if vm_display_name else "VM: Not Found"
                    # Fetch backend health details
                    backend_health = lb_client.get_backend_health(
                        load_balancer_id=lb_id,
                        backend_set_name=backend_set_name,
                        backend_name=backend.name
                    ).data
                    # Add backend health details to the report
                    health_report += f"  - Backend: {backend.ip_address}{vm_info}:{backend.port}, Health: {backend_health.status}, Offline: {backend.offline}, Weight: {backend.weight}\n"
        else:
            health_report += "No backend sets found for this load balancer.\n"
            logs.append("[WARN] No backend sets found for this load balancer.")

        # Send the health report via email
        notification_topic_id = os.environ.get("wlsc_email_notification_topic_id")
        if notification_topic_id:
            subject = f"Load Balancer Health Report - {auto_scale_env}"
            send_email(
                signer=signer,
                topic_id=notification_topic_id,
                email_body=health_report,
                subject=subject
            )
            logs.append("[INFO] Health report email sent successfully.")

        return response.Response(ctx, response_data=json.dumps({"logs": logs}), headers={"Content-Type": "application/json"})
    except Exception as e:
        logs.append(f"[ERROR] Failed to check load balancer health or send email. Error: {str(e)}")
        return response.Response(ctx, response_data=json.dumps({"logs": logs}), headers={"Content-Type": "application/json"})


def send_email(signer, topic_id, email_body=None, subject=""):
    """
    Sends an email to the notification topic.
    """
    try:
        ons_client = oci.ons.NotificationDataPlaneClient(config={}, signer=signer)
        message_details = oci.ons.models.MessageDetails(
            body=email_body,
            title=subject
        )
        ons_client.publish_message(topic_id, message_details=message_details, message_type="RAW_TEXT")
        logging.info("[INFO] Email sent successfully.")
    except Exception as e:
        logging.error(f"[ERROR] Failed to send email. Error: {str(e)}")


def get_vm_display_name_by_ip(ip_address, compartment_id, signer):
    """
    Fetches the VM display name using the IP address.
    """
    try:
        # Initialize the Compute and Virtual Network clients
        compute_client = oci.core.ComputeClient(config={}, signer=signer)
        network_client = oci.core.VirtualNetworkClient(config={}, signer=signer)
        # List all VNIC attachments in the compartment
        vnic_attachments = compute_client.list_vnic_attachments(compartment_id=compartment_id).data
        for vnic_attachment in vnic_attachments:
            # Get the VNIC details
            vnic = network_client.get_vnic(vnic_attachment.vnic_id).data
            # Check if the VNIC's private IP matches the given IP address
            if vnic.private_ip == ip_address:
                # Fetch the instance details
                instance = compute_client.get_instance(vnic_attachment.instance_id).data
                return instance.display_name
        return None  # Return None if no matching VM is found
    except Exception as e:
        logging.error(f"[ERROR] Failed to fetch VM display name for IP {ip_address}. Error: {str(e)}")
        return None
