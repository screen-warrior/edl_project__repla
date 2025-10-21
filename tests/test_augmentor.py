import pytest

from models.augmentation_model import EDL_Augmentor
from models.schemas import ValidatedEntry, EntryType, ValidationErrorDetail


def make_entry(value: str, entry_type=EntryType.FQDN, valid=True):
    return ValidatedEntry(
        source="test",
        original=value,
        normalized=value,
        entry_type=entry_type,
        error=None if valid else ValidationErrorDetail(code="invalid", message="", hint=None),
        meta={},
    )


def test_fqdn_strip_path_augmented():
    cfg = {
        "fqdn_augmenter": {
            "strip_path": {"enabled": True},
        }
    }
    augmentor = EDL_Augmentor(cfg)
    entry = make_entry("example.com/path/to/resource")

    augmented = augmentor.augment_entry(entry)

    assert augmented.augmented == "example.com"
    assert "strip_path" in augmented.changes


def test_fqdn_strip_path_trailing_slash():
    cfg = {
        "fqdn_augmenter": {
            "strip_path": {"enabled": True},
        }
    }
    augmentor = EDL_Augmentor(cfg)
    entry = make_entry("example.com/")

    augmented = augmentor.augment_entry(entry)

    assert augmented.augmented == "example.com"
    assert "strip_path" in augmented.changes
