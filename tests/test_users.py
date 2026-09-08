def test_create_user(client):
    r = client.post("/users", json={"name": "Alice"})
    assert r.status_code == 201
    body = r.json()
    assert body["name"] == "Alice"
    assert isinstance(body["id"], int)
    assert "created_at" in body


def test_get_user(client):
    created = client.post("/users", json={"name": "Bob"}).json()
    r = client.get(f"/users/{created['id']}")
    assert r.status_code == 200
    assert r.json()["id"] == created["id"]


def test_list_users(client):
    client.post("/users", json={"name": "Alice"})
    client.post("/users", json={"name": "Bob"})
    r = client.get("/users")
    assert r.status_code == 200
    names = [u["name"] for u in r.json()]
    assert names == ["Alice", "Bob"]


def test_update_user(client):
    created = client.post("/users", json={"name": "Carol"}).json()
    r = client.put(f"/users/{created['id']}", json={"name": "Caroline"})
    assert r.status_code == 200
    assert r.json()["name"] == "Caroline"


def test_delete_user(client):
    created = client.post("/users", json={"name": "Dave"}).json()
    r = client.delete(f"/users/{created['id']}")
    assert r.status_code == 204
    assert client.get(f"/users/{created['id']}").status_code == 404


def test_get_missing_user_returns_404(client):
    r = client.get("/users/999999")
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "not_found"


def test_update_missing_user_returns_404(client):
    r = client.put("/users/999999", json={"name": "Ghost"})
    assert r.status_code == 404


def test_invalid_validation_returns_422(client):
    r = client.post("/users", json={"name": ""})
    assert r.status_code == 422
    assert r.json()["error"]["code"] == "validation_error"

    r2 = client.post("/users", json={})
    assert r2.status_code == 422


def test_request_id_is_echoed(client):
    r = client.get("/users", headers={"X-Request-ID": "abc-123"})
    assert r.headers["X-Request-ID"] == "abc-123"
