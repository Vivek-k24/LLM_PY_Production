
import json

class MetadataManager:
    def __init__(self, metadata_path):
        self.metadata_path = metadata_path
        self.metadata = None

    def load_metadata(self):
        """Load dataset metadata from a JSON file."""
        try:
            with open(self.metadata_path, "r") as file:
                self.metadata = json.load(file)
        except Exception as e:
            self.metadata = {}  # Initialize empty metadata if the file doesn't exist
            print(f"Metadata file not found or invalid. Initializing empty metadata. Error: {e}")

    def get_metadata(self):
        """Retrieve the loaded metadata."""
        if self.metadata is None:
            raise ValueError("Metadata not loaded. Call load_metadata first.")
        return self.metadata

    def add_dataset_metadata(self, dataset_name, columns):
        """Add metadata for a new dataset."""
        if dataset_name in self.metadata:
            raise ValueError(f"Dataset '{dataset_name}' already exists in metadata.")
        self.metadata[dataset_name] = {"columns": columns}

    def save_metadata(self):
        """Save updated metadata back to the JSON file."""
        try:
            with open(self.metadata_path, "w") as file:
                json.dump(self.metadata, file, indent=4)
        except Exception as e:
            raise ValueError(f"Error saving metadata: {e}")
