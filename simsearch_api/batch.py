"""Receive-time validation and deduplication for a submitted list of SMILES."""

from dataclasses import dataclass, field

from simsearch_api.chemistry import InvalidSmiles, Prepared


@dataclass
class Row:
    """One submitted SMILES and what became of it."""

    index: int
    smiles: str
    prepared: Prepared | None = None
    error: str | None = None

    @property
    def ok(self):
        return self.prepared is not None


@dataclass
class Batch:
    """Submitted rows, and the distinct molecules they reduce to."""

    rows: list[Row] = field(default_factory=list)
    unique: list[Prepared] = field(default_factory=list)
    assignment: dict[int, int] = field(default_factory=dict)

    @property
    def counts(self):
        failed = sum(1 for row in self.rows if not row.ok)

        return {
            "submitted": len(self.rows),
            "unique": len(self.unique),
            "searched": len(self.rows) - failed,
            "failed": failed,
        }


def prepare(smiles_list, chemistry, blank_error="no SMILES supplied"):
    """Validate, clean and deduplicate a submitted list."""

    batch = Batch()
    seen = {}

    for index, raw in enumerate(smiles_list):
        text = (raw or "").strip()
        row = Row(index=index, smiles=text)
        batch.rows.append(row)

        if not text:
            row.error = blank_error
            continue

        try:
            row.prepared = chemistry.prepare(text)
        except InvalidSmiles as error:
            row.error = str(error)
            continue

        key = row.prepared.key
        position = seen.get(key)

        if position is None:
            position = len(batch.unique)
            seen[key] = position
            batch.unique.append(row.prepared)

        batch.assignment[index] = position

    return batch
