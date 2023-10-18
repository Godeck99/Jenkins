import logging
from models.vhome import vhome_data, vhome_vcenter
from sqlalchemy.orm import sessionmaker
from utils.functions import vCenterAPI
from config.database import engine
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_vcenters(region):
    """Fetch vCenters for a given region."""
    with sessionmaker(bind=engine)() as session:
        return session.query(vhome_data.vcenter, vhome_data.region).filter(vhome_data.region == region).distinct().all()

def update_backup_status_for_vcenter(vcenter):
    """Update backup status for a single vCenter."""
    check = vCenterAPI(vcenter["name"])
    check.login()

    query = check.appliance_query("recovery/backup/job")
    if not query['value']:
        return "Failed"

    last_backup = query["value"][0]
    status_query = check.appliance_query(f"recovery/backup/job/{last_backup}")["value"]["state"]
    check.close_session()

    return "OK" if status_query == "SUCCEEDED" else "Failed"

def update_backup_status(region):
    """
    Update the backup status for a given region in the database.

    Parameters:
    - region (str): The region for which to update the backup status.

    Returns:
    None
    """
    vcenters = [{"name": vc, "region": reg} for vc, reg in fetch_vcenters(region)]
    for vcenter in vcenters:
        try:
            logging.info(f"Processing vCenter: {vcenter['name']}")
            result = update_backup_status_for_vcenter(vcenter)

            with sessionmaker(bind=engine)() as session:
                session.query(vhome_vcenter).filter(
                    vhome_vcenter.vcenter == vcenter["name"]).update(
                    {vhome_vcenter.backup: result}
                )
                session.commit()

            logging.info(f"Backup status for vCenter {vcenter['name']} updated to {result}")
        except Exception as e:
            logging.error(f"Error processing vCenter {vcenter['name']}: {e}")

# Example usage:
# update_backup_status("AME")

======================================================================================================

from models.vhome import vhome_data, vhome_vcenter
from sqlalchemy.orm import sessionmaker
from utils.functions import vCenterAPI
from config.database import engine
from datetime import datetime

def update_backup_status(region):

    Session = sessionmaker(bind=engine)
    session = Session()

    # Fetch vCenters for the given region
    vcenter_db = session.query(vhome_data.vcenter, vhome_data.region).filter(vhome_data.region == region).distinct().all()
    vcenters = [{"name": vc, "region": reg} for vc, reg in vcenter_db]

    for vcenter in vcenters:
        try:
            print(vcenter["name"])
            check = vCenterAPI(vcenter["name"])
            check.login()
            query = check.appliance_query("recovery/backup/job")

            if not query['value']:
                result = "Failed"
            else:
                last_backup = query["value"][0]
                today = datetime.now().strftime("%Y%m%d")
                check_backup = last_backup.split("-")[0]
                status_query = check.appliance_query(f"recovery/backup/job/{last_backup}")["value"]["state"]

                if status_query == "SUCCEEDED":
                    result = "OK"
                else:
                    result = "Failed"

            session.query(vhome_vcenter).filter(
                vhome_vcenter.vcenter == vcenter["name"]).update(
                {vhome_vcenter.backup: result}
            )
            session.commit()
            print(result)
        except Exception as e:
            print(f"Error processing vCenter {vcenter['name']}: {e}")
        finally:
            check.close_session()

    session.close()

# Example usage:
# update_backup_status("AME")

=======================================

import logging
from models.vhome import vhome_data, vhome_vcenter
from sqlalchemy.orm import sessionmaker
from utils.functions import vCenterAPI
from config.database import engine
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_vcenters(region):
    """Fetch vCenters for a given region."""
    with sessionmaker(bind=engine)() as session:
        return session.query(vhome_data.vcenter, vhome_data.region).filter(vhome_data.region == region).distinct().all()

def update_backup_status_for_vcenter(vcenter):
    """Update backup status for a single vCenter."""
    today = datetime.today().date()
    check = vCenterAPI(vcenter["name"])
    check.login()

    query = check.appliance_query("recovery/backup/job")
    if not query['value']:
        check.close_session()
        return "Failed"

    last_backup_date = datetime.fromisoformat(query["value"][0]["creation_time"]).date()
    status_query = check.appliance_query(f"recovery/backup/job/{last_backup_date}")["value"]["state"]
    check.close_session()

    if status_query == "SUCCEEDED" and last_backup_date == today:
        return "OK"
    else:
        return "Failed"

def update_backup_status(region):
    """
    Update the backup status for a given region in the database.

    Parameters:
    - region (str): The region for which to update the backup status.

    Returns:
    None
    """
    vcenters = [{"name": vc, "region": reg} for vc, reg in fetch_vcenters(region)]
    for vcenter in vcenters:
        try:
            logging.info(f"Processing vCenter: {vcenter['name']}")
            result = update_backup_status_for_vcenter(vcenter)

            with sessionmaker(bind=engine)() as session:
                session.query(vhome_vcenter).filter(
                    vhome_vcenter.vcenter == vcenter["name"]).update(
                    {vhome_vcenter.backup: result}
                )
                session.commit()

            logging.info(f"Backup status for vCenter {vcenter['name']} updated to {result}")
        except Exception as e:
            logging.error(f"Error processing vCenter {vcenter['name']}: {e}")

# Example usage:
# update_backup_status("AME")