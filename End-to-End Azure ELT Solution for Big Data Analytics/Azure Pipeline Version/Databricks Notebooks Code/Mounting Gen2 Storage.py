# Databricks notebook source
# Unmounting

mount_point = "/mnt/stedi"
if any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
    dbutils.fs.unmount(mount_point)

# COMMAND ----------

# Mounting using storage container path

storage_account_name = "js1wgustedi"
container_name = "stedi"
sas_token = "sp=racwdlmeop&st=2025-09-10T00:01:19Z&se=2025-10-10T08:16:19Z&spr=https&sv=2024-11-04&sr=c&sig=M20kddbm6Knj3OWlKaMkIXhcB6QHqNdUG6O9DMEoVNQ%3D"  

mount_point = "/mnt/stedi"

# Mount if not already mounted
if not any(m.mountPoint == mount_point for m in dbutils.fs.mounts()):
    dbutils.fs.mount(
        source = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/",
        mount_point = mount_point,
        extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net": sas_token}
    )

# List files to confirm
display(dbutils.fs.ls("/mnt/stedi"))