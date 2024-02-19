#!/bin/bash

SRC_DIR="/home/iabhilashjoshi/My-Website"
DEST_DIR="/var/www/My-Website"

sync_files() {
    sudo rsync -av --delete "$SRC_DIR/" "$DEST_DIR/"
}

echo "Synchronizing files..."
sync_files
echo "Synchronization complete."

