
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
            raise ValueError(f"Error loading metadata: {e}")

    def get_metadata(self):
        """Retrieve the loaded metadata."""
        if not self.metadata:
            raise ValueError("Metadata not loaded. Call load_metadata first.")
        return self.metadata