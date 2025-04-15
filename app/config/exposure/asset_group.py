from dataclasses import dataclass
from typing import List
from config.exposure.asset import Asset

@dataclass
class AssetGroup:
    """
    A collection of assets that form a logical group for climate risk analysis.
    """
    name: str
    label: str
    description: str
    assets: List[Asset]
    icon: str = None
    color: str = "#6a6a6a"