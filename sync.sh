#!/bin/bash

SRC_DIR="/home/iabhilashjoshi/abhi/lash/My-Website"
DEST_DIR="/home/iabhilashjoshi/My-Website"

sync_files() {
    rsync -av --delete "$SRC_DIR/" "$DEST_DIR/"
}

echo "Synchronizing files..."
sync_files
echo "Synchronization complete."

