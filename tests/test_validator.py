import pytest
from app.validator import validate_record

def test_valid_record():
    ok, payload = validate_record({
        "customerId": "1",
        "name": "John Doe",
        "email": "john@example.com",
        "createdAt": "2024-03-26T12:00:00Z"
    })
    assert ok is True

@pytest.mark.parametrize("bad", [
    {"customerId":"", "name":"A","email":"a@example.com","createdAt":"2024-03-26T12:00:00Z"},
    {"customerId":"1", "name":"  ","email":"a@example.com","createdAt":"2024-03-26T12:00:00Z"},
    {"customerId":"1", "name":"A","email":"invalid.email","createdAt":"2024-03-26T12:00:00Z"},
    {"customerId":"1", "name":"A","email":"john@example.com","createdAt":"2024-0326"},
    {"name":"A","email":"john@example.com","createdAt":"2024-03-26T12:00:00Z"},  # missing customerId
])
def test_invalid_records(bad):
    ok, err = validate_record(bad)
    print(bad)
    assert ok is False
    assert err["status"] == "error"
    assert "reason" in err
