
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

    def add_dataset_metadata(self, dataset_name, columns, purpose, time_filter_column=None, date_filter_column=None,  replace= False):
        """
        Add metadata for a new dataset.
        - dataset_name: The name of the dataset.
        - columns: The columns of the dataset with descriptions.
        - purpose: The purpose of the dataset.
        - time_filter_column: The default time filter column for the dataset.
        """
        if dataset_name in self.metadata and not replace:
            raise ValueError(f"Dataset '{dataset_name}' already exists in metadata.")
        if dataset_name in self.metadata and replace:
            print(f"Replacing existing metadata for dataset '{dataset_name}'")

        self.metadata[dataset_name] = {
            "dataset_name": dataset_name,
            "columns": columns,
            "time_filter_column": time_filter_column,
            "date_filter_column": date_filter_column,
            "purpose": purpose
        }

    def save_metadata(self):
        """Save updated metadata back to the JSON file."""
        try:
            with open(self.metadata_path, "w") as file:
                json.dump(self.metadata, file, indent=4)
        except Exception as e:
            raise ValueError(f"Error saving metadata: {e}")
