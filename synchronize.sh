#!/bin/bash

# Base destination directory
BASE_DEST_DIR="/var/www"

# Full source directory path
SRC_DIR="$PWD"

# Get the end directory name from the current working directory
END_DIR=$(basename "$SRC_DIR")


# Full destination directory path
DEST_DIR="$BASE_DEST_DIR/$END_DIR"

# Full backup directory path using the user's home directory
BACKUP_DIR="$HOME/.deployment_backup"

# Function to synchronize files
sync_files() {
   echo "Source directory: $SRC_DIR"
   echo "Destination directory: $DEST_DIR"

   # Check if DEST_DIR exists
   if [ -d "$DEST_DIR" ]; then
       # Backup existing content if DEST_DIR exists
       backup_destination
   else
       # Create DEST_DIR if it doesn't exist
       mkdir -p "$DEST_DIR"
       echo "Destination directory created: $DEST_DIR"
   fi

   # Sync files from SRC_DIR to DEST_DIR
   sudo rsync -av --delete "$SRC_DIR/" "$DEST_DIR/"
   echo "Synchronization complete."
   echo "To rollback changes, run: $0 rollback"
}

# Function to backup destination directory
backup_destination() {
   # Create backup directory if it doesn't exist
   if [ ! -d "$BACKUP_DIR/$END_DIR" ]; then
       mkdir -p "$BACKUP_DIR/$END_DIR"
       echo "Backup directory created: $BACKUP_DIR/$END_DIR"
   fi

   # Perform backup
   sudo rsync -av --delete "$DEST_DIR/" "$BACKUP_DIR/$END_DIR/"
   echo "Backup created in $BACKUP_DIR/$END_DIR"
}

# Function to rollback changes
rollback() {
   echo "Rolling back changes..."
   echo "Rolling back changes for $DEST_DIR from $BACKUP_DIR/$END_DIR"
   if [ -d "$BACKUP_DIR/$END_DIR" ]; then
       sudo rsync -av --delete "$BACKUP_DIR/$END_DIR/" "$DEST_DIR/"
       echo "Rollback completed"
   else
       echo "No backup found"
   fi
}

# Manually trigger synchronization or rollback
if [ "$1" = "rollback" ]; then
   rollback
else
   # Synchronize files
   echo "Synchronizing files..."
   sync_files
fi

