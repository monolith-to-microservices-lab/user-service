def test_import_preserves_id(client):
    r = client.post(
        "/internal/users/import",
        json={"id": 37, "name": "Thiago", "created_at": "2020-01-01T00:00:00Z"},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "created"
    assert body["user"]["id"] == 37
    assert body["user"]["name"] == "Thiago"

    got = client.get("/users/37").json()
    assert got["id"] == 37


def test_import_is_idempotent(client):
    payload = {"id": 37, "name": "Thiago", "created_at": "2020-01-01T00:00:00Z"}
    first = client.post("/internal/users/import", json=payload)
    assert first.status_code == 201

    second = client.post("/internal/users/import", json=payload)
    assert second.status_code == 200
    assert second.json()["status"] == "unchanged"

    listed = client.get("/users").json()
    assert [u["id"] for u in listed] == [37]


def test_import_conflict_is_explicit(client):
    client.post("/internal/users/import", json={"id": 5, "name": "Original"})
    r = client.post("/internal/users/import", json={"id": 5, "name": "Different"})
    assert r.status_code == 409
    body = r.json()
    assert body["error"]["code"] == "import_conflict"
    assert body["error"]["conflicts"]["name"] == {
        "existing": "Original",
        "incoming": "Different",
    }
    # Nothing was overwritten.
    assert client.get("/users/5").json()["name"] == "Original"


def test_normal_create_after_high_id_import(client):
    client.post("/internal/users/import", json={"id": 1000, "name": "Legacy"})
    r = client.post("/users", json={"name": "Fresh"})
    assert r.status_code == 201
    new_id = r.json()["id"]
    assert new_id == 1001

    # And no collision on a subsequent create.
    r2 = client.post("/users", json={"name": "Fresh2"})
    assert r2.json()["id"] == 1002


def test_import_rejects_non_positive_id(client):
    r = client.post("/internal/users/import", json={"id": 0, "name": "Nope"})
    assert r.status_code == 422
