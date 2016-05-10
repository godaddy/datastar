CREATE TABLE IF NOT EXISTS artist (
  artist_id uuid,
  name text,
  create_date timestamp,
  update_date timestamp,
  members set<text>,
  related_artists set<uuid>,
  traits set<text>,
  metadata map<text,text>,
  PRIMARY KEY (artist_id)
);
