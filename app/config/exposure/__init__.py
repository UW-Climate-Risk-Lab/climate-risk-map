from typing import List, Dict, Optional

from config.exposure.asset import (
    Asset,
    AssetRegistry,
    TRANSPARENT_MARKER_CLUSTER,
    CREATE_FEATURE_ICON,
    CREATE_FEATURE_COLOR_STYLE
)
from config.exposure.asset_group import AssetGroup
from config.exposure.definitions import (
    load_asset_definitions,
    load_asset_group_definitions,
    
)


# --- Centralized Initialization Logic ---

# Module-level storage for the initialized objects
_ASSETS_BY_NAME: Dict[str, Asset] = {}
_ASSETS_BY_LABEL: Dict[str, Asset] = {}

_ASSET_GROUPS_BY_NAME: Dict[str, AssetGroup] = {}
_ASSET_GROUPS_BY_LABEL: Dict[str, AssetGroup] = {}

ALL_ASSETS: List[Asset] = []
ALL_ASSET_GROUPS: List[AssetGroup] = []


def initialize_asset_config():
    """Loads definitions and creates all Asset and AssetGroup objects."""
    global _ASSETS_BY_NAME, _ASSET_GROUPS_BY_NAME, ALL_ASSETS, ALL_ASSET_GROUPS

    # Clear existing data if re-initializing
    _ASSETS_BY_NAME.clear()
    _ASSET_GROUPS_BY_NAME.clear()
    ALL_ASSETS.clear()
    ALL_ASSET_GROUPS.clear()

    asset_definitions = load_asset_definitions()
    asset_group_definitions = load_asset_group_definitions()

    # Phase 1: Create all Asset objects
    for name, config in asset_definitions.items():
        asset_type = config.pop("type", None)  # Use pop with default
        if not asset_type:
            print(f"Warning: Asset definition '{name}' missing 'type'. Skipping.")
            continue
        try:
            # Ensure required fields like 'label' are present or handle gracefully
            if "label" not in config:
                print(
                    f"Warning: Asset definition '{name}' missing 'label'. Using name as label."
                )
                config["label"] = name  # Default label to name if missing

            # Add name back into config dict before passing to constructor
            config_with_name = {"name": name, **config}
            asset = AssetRegistry.create_asset(asset_type, **config_with_name)
            _ASSETS_BY_NAME[name] = asset
            _ASSETS_BY_LABEL[config["label"]] = asset
            ALL_ASSETS.append(asset)
        except (ValueError, TypeError) as e:
            print(f"Error creating asset '{name}': {e}. Skipping.")
            # Add more specific error handling if needed based on AssetRegistry or dataclass issues
        except KeyError as e:
            print(
                f"Error creating asset '{name}': Missing required field {e}. Skipping."
            )

    # Phase 2: Create all AssetGroup objects
    for name, config in asset_group_definitions.items():
        try:
            asset_names = config.get("assets", [])
            # Look up Asset objects using the names
            group_assets = []
            missing_assets = []
            for asset_name in asset_names:
                asset = _ASSETS_BY_NAME.get(asset_name)
                if asset:
                    group_assets.append(asset)
                else:
                    missing_assets.append(asset_name)

            if missing_assets:
                print(
                    f"Warning: AssetGroup '{name}' references missing assets: {', '.join(missing_assets)}"
                )
                # Decide if you want to create the group anyway or skip it
                # continue # Uncomment to skip groups with missing assets

            # Prepare config for AssetGroup, removing 'assets' list of names
            group_config = config.copy()
            group_config.pop("assets", None)

            # Create the group with the actual Asset objects
            asset_group = AssetGroup(name=name, assets=group_assets, **group_config)
            _ASSET_GROUPS_BY_NAME[name] = asset_group
            _ASSET_GROUPS_BY_LABEL[config["label"]] = asset_group
            ALL_ASSET_GROUPS.append(asset_group)
        except Exception as e:  # Catch broader exceptions during group creation
            print(f"Error creating asset group '{name}': {e}. Skipping.")


# --- Accessor Functions ---


def get_asset(name: str) -> Optional[Asset]:
    """Returns the Asset object by its unique name or label."""
    asset = _ASSETS_BY_NAME.get(name, None)

    if not asset:
        asset = _ASSETS_BY_LABEL.get(name, None)

    return asset


def get_asset_group(name: str) -> Optional[AssetGroup]:
    """Returns the AssetGroup object by its unique name or label."""

    asset_group = _ASSET_GROUPS_BY_NAME.get(name, None)

    if not asset_group:
        asset_group = _ASSET_GROUPS_BY_LABEL.get(name, None)

    return asset_group


def get_all_assets() -> List[Asset]:
    """Returns a list of all configured Asset objects."""
    return ALL_ASSETS[:]  # Return a copy


def get_all_asset_groups() -> List[AssetGroup]:
    """Returns a list of all configured AssetGroup objects."""
    return ALL_ASSET_GROUPS[:]  # Return a copy


# --- Initialize on Import ---
# This ensures assets and groups are ready when the module is imported elsewhere.
initialize_asset_config()


__all__ = [
    "get_asset",
    "get_asset_group",
    "get_all_assets",
    "get_all_asset_groups",
    "Asset",
    "AssetGroup",
    "TRANSPARENT_MARKER_CLUSTER",
    "CREATE_FEATURE_ICON",
    "CREATE_FEATURE_COLOR_STYLE"
]
