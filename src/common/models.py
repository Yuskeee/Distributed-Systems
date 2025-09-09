from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Dict, Any

@dataclass
class Auction:
    id: int
    description: str
    start_time: datetime
    end_time: datetime
    status: str  # "active" or "closed"

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Auction object into a dictionary (JSON serializable)."""
        data = asdict(self)
        # Convert datetime to ISO format
        data["start_time"] = self.start_time.isoformat()
        data["end_time"] = self.end_time.isoformat()
        return data

@dataclass
class Bid:
    auction_id: int
    user_id: str
    amount: float
    signature: str  # digital signature of the bid

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Bid object into a dictionary (JSON serializable)."""
        return asdict(self)


@dataclass
class Message:
    event_type: str
    payload: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        """Convert the Message object into a dictionary (JSON serializable)."""
        return asdict(self)
