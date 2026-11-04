from pydantic import BaseModel, Field


class Node(BaseModel):
    """A stored node record.

    Attributes:
        node_id: Server-assigned stable identifier.
        source: The system that owns this record (e.g., 'cds').
        source_id: The stable identifier in the source system.
        host: Hostname or IPv4/IPv6 address used to reach the node.
        port: Port used to reach the node.
        community: SNMP community string.
        name: Human-readable name.
        make: Hardware manufacturer.
        model: Hardware model.
    """

    node_id: str
    source: str | None = None
    source_id: str | None = None
    host: str
    port: int
    community: str
    name: str
    make: str
    model: str


class NodeCreateRequest(BaseModel):
    """Client-supplied fields; node_id is assigned by the server on insert."""

    source: str | None = None
    source_id: str | None = None
    host: str = Field(min_length=1)
    port: int = Field(gt=0, le=65535)
    community: str = Field(min_length=1)
    name: str = Field(min_length=1)
    make: str = Field(min_length=1)
    model: str = Field(min_length=1)

class NodeUpdateRequest(BaseModel):
    """Full replacement body for PUT; node_id comes from the path."""

    source: str | None = None
    source_id: str | None = None
    host: str = Field(min_length=1)
    port: int = Field(gt=0, le=65535)
    community: str = Field(min_length=1)
    name: str = Field(min_length=1)
    make: str = Field(min_length=1)
    model: str = Field(min_length=1)
