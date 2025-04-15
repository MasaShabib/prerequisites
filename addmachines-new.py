import csv
import subprocess
import json
import time
import argparse
from concurrent.futures import ThreadPoolExecutor

def get_machine_status(maas_user, system_id):
    status_command = ["maas", maas_user, "machine", "read", system_id]
    result = subprocess.run(status_command, capture_output=True, text=True)
    if result.returncode == 0:
        machine_info = json.loads(result.stdout)
        return machine_info.get("status_name", "Unknown")
    return "Unknown"

def wait_for_status(maas_user, system_id, expected_status, hostname, timeout=1800, interval=10):
    elapsed_time = 0
    while elapsed_time < timeout:
        status = get_machine_status(maas_user, system_id)
        print(f"Current status of {hostname}: {status}")
        if status == expected_status:
            return True
        time.sleep(interval)
        elapsed_time += interval
    print(f"Timeout waiting for machine {hostname} to reach {expected_status}.")
    return False

def create_machine(maas_user, row):
    hostname = row["hostname"]
    architecture = row["architecture"]
    mac_addresses = row["mac_addresses"]
    power_type = row["power_type"]
    power_parameters = {
        "power_user": row["power_user"],
        "power_pass": row["power_pass"],
        "power_driver": row["power_driver"],
        "power_address": row["power_address"],
        "cipher_suite_id": row["cipher_suite_id"],
        "power_boot_type": row["power_boot_type"],
        "privilege_level": row["privilege_level"],
        "k_g": row["k_g"]
    }

    create_command = [
        "maas", maas_user, "machines", "create",
        f"hostname={hostname}",
        f"architecture={architecture}",
        f"mac_addresses={mac_addresses}",
        f"power_type={power_type}",
        f"power_parameters={json.dumps(power_parameters)}"
    ]

    try:
        result = subprocess.run(create_command, check=True, capture_output=True, text=True)
        print(f"Successfully added {hostname}")
        response_json = json.loads(result.stdout)
        return hostname, response_json.get("system_id"), row["power_user"], row["power_pass"], row["power_address"]
    except Exception as e:
        print(f"Error creating {hostname}: {str(e)}")
        return hostname, None, None, None, None

def apply_cloud_init(maas_user, system_id, cloud_init_file):
    try:
        with open(cloud_init_file, "r") as f:
            user_data = f.read()
        cloud_init_command = [
            "maas", maas_user, "machine", "update", system_id,
            f"user_data={json.dumps(user_data)}"
        ]
        subprocess.run(cloud_init_command, check=True, capture_output=True, text=True)
        print(f"Applied cloud-init to {system_id}")
    except Exception as e:
        print(f"Failed to apply cloud-init for {system_id}: {str(e)}")

def deploy_machine(maas_user, hostname, system_id):
    try:
        deploy_command = ["maas", maas_user, "machine", "deploy", system_id]
        subprocess.run(deploy_command, check=True, capture_output=True, text=True)
        print(f"Deployed {hostname}")
    except Exception as e:
        print(f"Failed to deploy {hostname}: {str(e)}")

def configure_and_deploy(maas_user, hostname, system_id, ipmi_user, ipmi_pass, ipmi_address, cloud_init_file):
    if not system_id:
        print(f"Skipping {hostname} due to missing system_id.")
        return

    if wait_for_status(maas_user, system_id, "Ready", hostname):
        if cloud_init_file:
            apply_cloud_init(maas_user, system_id, cloud_init_file)

        deploy_machine(maas_user, hostname, system_id)

        if wait_for_status(maas_user, system_id, "Deployed", hostname):
            print(f"{hostname} has been successfully deployed.")
        else:
            print(f"{hostname} did not reach 'Deployed' state.")
    else:
        print(f"Skipping deployment for {hostname} as it did not reach Ready state.")

def add_machines_from_csv(maas_user, csv_file, cloud_init_file):
    with open(csv_file, mode='r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(create_machine, maas_user, row) for row in rows]
        results = [future.result() for future in futures]

    with ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda args: configure_and_deploy(maas_user, *args, cloud_init_file), results)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add and deploy MAAS machines from a CSV file.")
    parser.add_argument("--maas_user", required=True, help="MAAS username")
    parser.add_argument("--csv_filename", required=True, help="Path to CSV file")
    parser.add_argument("--cloud_init_file", required=True, help="Path to cloud-init YAML file")
    args = parser.parse_args()

    add_machines_from_csv(args.maas_user, args.csv_filename, args.cloud_init_file)

