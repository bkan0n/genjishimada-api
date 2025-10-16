BEGIN;

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW IS DISTINCT FROM OLD THEN
    NEW.updated_at := now();
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS maps;
CREATE SCHEMA IF NOT EXISTS playtests;
CREATE SCHEMA IF NOT EXISTS users;
CREATE SCHEMA IF NOT EXISTS completions;
CREATE SCHEMA IF NOT EXISTS lootbox;
CREATE SCHEMA IF NOT EXISTS rank_card;


CREATE TABLE IF NOT EXISTS core.users
(
    id          bigint PRIMARY KEY,
    nickname    text,
    global_name text,
    coins       int         DEFAULT 0,
    created_at  timestamptz DEFAULT now(),
    updated_at  timestamptz DEFAULT now()
);

COMMENT ON COLUMN core.users.nickname IS 'Discord server-based nickname';
COMMENT ON COLUMN core.users.global_name IS 'Discord global username';
COMMENT ON COLUMN core.users.id IS 'Discord user ID snowflake';
COMMENT ON COLUMN core.users.coins IS 'Coins received from community contributions';

CREATE INDEX IF NOT EXISTS idx_users_nickname_trgm ON core.users USING gin (nickname gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_users_global_name_trgm ON core.users USING gin (global_name gin_trgm_ops);

CREATE TRIGGER update_core_users_updated_at
BEFORE UPDATE ON core.users
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TYPE playtest_status AS enum ('Approved', 'In Progress', 'Rejected');

CREATE TABLE IF NOT EXISTS core.maps
(
    id             int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    code           text UNIQUE
        CONSTRAINT overwatch_code CHECK (code ~ '[A-Z0-9]{4,6}') NOT NULL,
    map_name       text                                          NOT NULL,
    category       text                                          NOT NULL,
    checkpoints    integer                                       NOT NULL,
    official       bool            DEFAULT TRUE                  NOT NULL,
    playtesting    playtest_status DEFAULT 'In Progress'         NOT NULL,
    hidden         bool            DEFAULT TRUE                  NOT NULL,
    archived       bool            DEFAULT FALSE                 NOT NULL,
    difficulty     text                                          NOT NULL,
    raw_difficulty numeric(4, 2)                                 NOT NULL,
    description    text,
    created_at     timestamptz     DEFAULT now(),
    updated_at     timestamptz     DEFAULT now(),
    custom_banner  text,
    title          text CHECK (char_length(title) <= 50),
    linked_code    text
        REFERENCES core.maps(code)
        ON UPDATE CASCADE
        ON DELETE SET NULL,
        CONSTRAINT maps_linked_code_not_self
        CHECK (linked_code IS NULL OR linked_code <> code)
);
COMMENT ON COLUMN core.maps.code IS 'Overwatch custom games share code';
COMMENT ON COLUMN core.maps.map_name IS 'Overwatch map name';
COMMENT ON COLUMN core.maps.category IS 'Type of Genji Parkour map';
COMMENT ON COLUMN core.maps.checkpoints IS 'The count of checkpoints in the parkour map';
COMMENT ON COLUMN core.maps.official IS 'If the map will/has gone through a playtesting process as opposed to most Chinese (or any other) maps outside of our influence';
COMMENT ON COLUMN core.maps.playtesting IS 'If the map is currently in playtesting or approved or rejected';
COMMENT ON COLUMN core.maps.hidden IS 'If the map is currently hidden from searches or not';
COMMENT ON COLUMN core.maps.archived IS 'If the map has been archived';
COMMENT ON COLUMN core.maps.description IS 'Optional description';
COMMENT ON COLUMN core.maps.difficulty IS 'String representation of the difficulty';
COMMENT ON COLUMN core.maps.raw_difficulty IS 'Numeric representation of the difficulty';
COMMENT ON COLUMN core.maps.custom_banner IS 'URL to a custom banner for a map instead of using the default.';
COMMENT ON COLUMN core.maps.title IS 'A custom title for the map.';

CREATE INDEX IF NOT EXISTS idx_maps_category ON core.maps (category);
CREATE INDEX IF NOT EXISTS idx_maps_playtesting ON core.maps (playtesting);
CREATE INDEX IF NOT EXISTS idx_maps_official ON core.maps (official);
CREATE INDEX IF NOT EXISTS idx_maps_archived ON core.maps (archived);
CREATE INDEX IF NOT EXISTS idx_maps_difficulty ON core.maps (difficulty);
CREATE INDEX IF NOT EXISTS idx_maps_raw_difficulty ON core.maps (raw_difficulty);

CREATE TRIGGER update_core_maps_updated_at
BEFORE UPDATE ON core.maps
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE OR REPLACE FUNCTION core.set_maps_difficulty_from_raw()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.raw_difficulty IS NULL THEN
    NEW.difficulty := NULL;
    RETURN NEW;
  END IF;

  CASE
    WHEN NEW.raw_difficulty >= 0.00 AND NEW.raw_difficulty < 1.18 THEN NEW.difficulty := 'Easy -';
    WHEN NEW.raw_difficulty >= 1.18 AND NEW.raw_difficulty < 1.76 THEN NEW.difficulty := 'Easy';
    WHEN NEW.raw_difficulty >= 1.76 AND NEW.raw_difficulty < 2.35 THEN NEW.difficulty := 'Easy +';
    WHEN NEW.raw_difficulty >= 2.35 AND NEW.raw_difficulty < 2.94 THEN NEW.difficulty := 'Medium -';
    WHEN NEW.raw_difficulty >= 2.94 AND NEW.raw_difficulty < 3.53 THEN NEW.difficulty := 'Medium';
    WHEN NEW.raw_difficulty >= 3.53 AND NEW.raw_difficulty < 4.12 THEN NEW.difficulty := 'Medium +';
    WHEN NEW.raw_difficulty >= 4.12 AND NEW.raw_difficulty < 4.71 THEN NEW.difficulty := 'Hard -';
    WHEN NEW.raw_difficulty >= 4.71 AND NEW.raw_difficulty < 5.29 THEN NEW.difficulty := 'Hard';
    WHEN NEW.raw_difficulty >= 5.29 AND NEW.raw_difficulty < 5.88 THEN NEW.difficulty := 'Hard +';
    WHEN NEW.raw_difficulty >= 5.88 AND NEW.raw_difficulty < 6.47 THEN NEW.difficulty := 'Very Hard -';
    WHEN NEW.raw_difficulty >= 6.47 AND NEW.raw_difficulty < 7.06 THEN NEW.difficulty := 'Very Hard';
    WHEN NEW.raw_difficulty >= 7.06 AND NEW.raw_difficulty < 7.65 THEN NEW.difficulty := 'Very Hard +';
    WHEN NEW.raw_difficulty >= 7.65 AND NEW.raw_difficulty < 8.24 THEN NEW.difficulty := 'Extreme -';
    WHEN NEW.raw_difficulty >= 8.24 AND NEW.raw_difficulty < 8.82 THEN NEW.difficulty := 'Extreme';
    WHEN NEW.raw_difficulty >= 8.82 AND NEW.raw_difficulty < 9.41 THEN NEW.difficulty := 'Extreme +';
    WHEN NEW.raw_difficulty >= 9.41 AND NEW.raw_difficulty <= 10.00 THEN NEW.difficulty := 'Hell';
    ELSE
      RAISE EXCEPTION 'raw_difficulty % is out of expected range [0.00, 10.00]', NEW.raw_difficulty
        USING ERRCODE = '22003';
  END CASE;

  RETURN NEW;
END;
$$;

-- Trigger: fire only when raw_difficulty is inserted/changed
DROP TRIGGER IF EXISTS trg_maps_set_difficulty_from_raw ON core.maps;
CREATE TRIGGER trg_maps_set_difficulty_from_raw
BEFORE INSERT OR UPDATE OF raw_difficulty
ON core.maps
FOR EACH ROW
EXECUTE FUNCTION core.set_maps_difficulty_from_raw();


CREATE TABLE IF NOT EXISTS maps.mechanics
(
    id       int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name     text UNIQUE NOT NULL,
    position int UNIQUE
);
COMMENT ON COLUMN maps.mechanics.position IS 'Visual ordering position for consistency';

CREATE TABLE IF NOT EXISTS maps.restrictions
(
    id       int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name     text UNIQUE NOT NULL,
    position int UNIQUE
);
COMMENT ON COLUMN maps.restrictions.position IS 'Visual ordering position for consistency';

CREATE TABLE IF NOT EXISTS maps.mechanic_links
(
    map_id      int REFERENCES core.maps (id) ON DELETE CASCADE,
    mechanic_id int REFERENCES maps.mechanics (id) ON DELETE CASCADE,
    PRIMARY KEY (map_id, mechanic_id)
);
CREATE INDEX IF NOT EXISTS idx_mechanic_links_mechanic_id ON maps.mechanic_links (mechanic_id);

CREATE TABLE IF NOT EXISTS maps.restriction_links
(
    map_id         int REFERENCES core.maps (id) ON DELETE CASCADE,
    restriction_id int REFERENCES maps.restrictions (id) ON DELETE CASCADE,
    PRIMARY KEY (map_id, restriction_id)
);
CREATE INDEX IF NOT EXISTS idx_restriction_links_restriction_id ON maps.restriction_links (restriction_id);

CREATE TABLE IF NOT EXISTS maps.creators
(
    map_id     int REFERENCES core.maps (id) ON DELETE CASCADE,
    user_id    bigint REFERENCES core.users (id) ON DELETE CASCADE,
    is_primary boolean DEFAULT FALSE,
    PRIMARY KEY (map_id, user_id)
);
COMMENT ON COLUMN maps.creators.is_primary IS 'There can only be one primary creator';

CREATE INDEX idx_creators_user_id ON maps.creators (user_id);
CREATE INDEX idx_creators_user_primary ON maps.creators (user_id, is_primary);

CREATE TABLE IF NOT EXISTS maps.medals
(
    map_id int PRIMARY KEY REFERENCES core.maps (id) ON DELETE CASCADE,
    gold   numeric(10, 2),
    silver numeric(10, 2),
    bronze numeric(10, 2)
);

CREATE TABLE IF NOT EXISTS maps.guides
(
    id      int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    map_id  int REFERENCES core.maps (id) ON DELETE CASCADE,
    url     text NOT NULL,
    user_id bigint REFERENCES core.users (id) ON DELETE CASCADE
);

ALTER TABLE maps.guides
ADD CONSTRAINT guides_user_id_map_id_unique
UNIQUE (user_id, map_id);


CREATE TABLE IF NOT EXISTS maps.clicks (
    id          bigserial PRIMARY KEY,
    map_id      int NOT NULL REFERENCES core.maps (id) ON DELETE CASCADE,
    user_id     bigint REFERENCES core.users (id),
    source      text,                    -- e.g. 'web', 'bot', 'embed'
    user_agent  text,
    ip_hash     text,                    -- optional: SHA256(ip + server_salt)
    inserted_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_clicks_map_time ON maps.clicks (map_id, inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_clicks_recent ON maps.clicks (inserted_at DESC);

ALTER TABLE maps.clicks
    ADD COLUMN day_bucket bigint;

-- 3) Create a trigger to keep it filled on INSERT/UPDATE
CREATE OR REPLACE FUNCTION maps.set_clicks_day_bucket()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    -- bucket by UTC day: seconds since epoch / 86400
    NEW.day_bucket := (extract(epoch FROM NEW.inserted_at)::bigint / 86400);
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_clicks_day_bucket_ins ON maps.clicks;
CREATE TRIGGER trg_clicks_day_bucket_ins
    BEFORE INSERT ON maps.clicks
    FOR EACH ROW
EXECUTE FUNCTION maps.set_clicks_day_bucket();

ALTER TABLE maps.clicks
    ADD CONSTRAINT u_click_unique_per_day
        UNIQUE (map_id, ip_hash, day_bucket);


CREATE TABLE IF NOT EXISTS maps.ratings
(
    id         int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    map_id     int REFERENCES core.maps (id) ON DELETE CASCADE,
    user_id    bigint REFERENCES core.users (id) ON DELETE CASCADE,
    quality    int
        CONSTRAINT quality_range CHECK (quality BETWEEN 1 AND 10),
    verified   boolean     DEFAULT FALSE,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),
    UNIQUE (map_id, user_id)
);
COMMENT ON COLUMN maps.ratings.quality IS 'A quality rating of the map 1 through 6';

CREATE INDEX IF NOT EXISTS idx_ratings_map_id_quality ON maps.ratings (map_id, quality);
CREATE INDEX IF NOT EXISTS idx_ratings_map_id_quality_verified ON maps.ratings (map_id, quality, verified);

CREATE TRIGGER update_maps_ratings_updated_at
BEFORE UPDATE ON maps.ratings
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();



CREATE TABLE IF NOT EXISTS playtests.meta
(
    id                                  int GENERATED ALWAYS AS IDENTITY,
    thread_id                           bigint UNIQUE,
    map_id                              int REFERENCES core.maps (id) ON DELETE CASCADE,
    verification_id                     bigint,
    initial_difficulty                  numeric(4, 2)
        CONSTRAINT difficulty_range CHECK (initial_difficulty >= 0 AND initial_difficulty <= 10) NOT NULL,
    created_at                          timestamptz DEFAULT now(),
    updated_at                          timestamptz DEFAULT now(),
    completed                           boolean     DEFAULT FALSE,
    PRIMARY KEY (id, map_id)
);
COMMENT ON COLUMN playtests.meta.thread_id IS 'Playtest forum post thread id snowflake in Discord';
COMMENT ON COLUMN playtests.meta.verification_id IS 'Playtest verification queue message id snowflake in Discord';
COMMENT ON COLUMN playtests.meta.initial_difficulty IS 'The difficulty value the creator believes the map to be';
COMMENT ON COLUMN playtests.meta.completed IS 'If the playtest has concluded';

CREATE TRIGGER update_playtests_meta_updated_at
BEFORE UPDATE ON playtests.meta
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS playtests.votes
(
    id                 int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id            bigint REFERENCES core.users (id) ON DELETE CASCADE,
    map_id             int REFERENCES core.maps (id) ON DELETE CASCADE,
    playtest_thread_id bigint REFERENCES playtests.meta (thread_id) ON DELETE CASCADE NOT NULL,
    difficulty         numeric(4, 2)
        CONSTRAINT difficulty_range CHECK (difficulty >= 0 AND difficulty <= 10)      NOT NULL,
    created_at         timestamptz DEFAULT now(),
    updated_at         timestamptz DEFAULT now()
);
COMMENT ON COLUMN playtests.votes.difficulty IS 'Difficulty value the user voted for';
CREATE UNIQUE INDEX IF NOT EXISTS votes_user_id_map_id_playtest_thread_id_uindex ON playtests.votes (user_id, map_id, playtest_thread_id);


CREATE OR REPLACE FUNCTION playtests.enforce_verified_completion_immediate()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM core.completions c
    WHERE c.user_id = NEW.user_id
      AND c.map_id  = NEW.map_id
      AND c.verified = TRUE
      AND c.legacy   = FALSE
  ) THEN
    RAISE EXCEPTION
      'User % has no verified non-legacy completion for map %',
      NEW.user_id, NEW.map_id
      USING ERRCODE = '23514'; -- check_violation
  END IF;

  RETURN NEW;
END
$$;

CREATE TRIGGER votes_requires_verified_completion
BEFORE INSERT OR UPDATE OF user_id, map_id
ON playtests.votes
FOR EACH ROW
EXECUTE FUNCTION playtests.enforce_verified_completion_immediate();





CREATE TRIGGER update_playtests_votes_updated_at
BEFORE UPDATE ON playtests.votes
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS playtests.stale_alerts
(
    user_id            bigint REFERENCES core.users (id) ON DELETE CASCADE,
    map_id             int REFERENCES core.maps (id) ON DELETE CASCADE,
    playtest_thread_id bigint REFERENCES playtests.meta (thread_id) ON DELETE CASCADE NOT NULL,
    created_at         timestamptz DEFAULT now(),
    alerted            boolean     DEFAULT FALSE,
    approved           boolean     DEFAULT FALSE,
    PRIMARY KEY (user_id, map_id)
);
COMMENT ON COLUMN playtests.stale_alerts.alerted IS 'Whether a stale alert has occurred';
COMMENT ON COLUMN playtests.stale_alerts.approved IS 'If the playtest has been approved, no more alerts needed';


CREATE TABLE IF NOT EXISTS playtests.deprecated_count (
  user_id bigint PRIMARY KEY,
  count   bigint NOT NULL CHECK (count >= 0)
);

-- Optional: index for reporting by gap size
CREATE INDEX IF NOT EXISTS deprecated_count_count_idx
  ON playtests.deprecated_count (count DESC);


CREATE TABLE IF NOT EXISTS users.notification_settings
(
    user_id bigint PRIMARY KEY REFERENCES core.users (id) ON DELETE CASCADE,
    flags   integer DEFAULT 0
);
COMMENT ON COLUMN users.notification_settings.flags IS 'Bitwise flag for allowed notifications';


CREATE TABLE IF NOT EXISTS users.overwatch_usernames
(
    id         int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    bigint REFERENCES core.users (id) ON DELETE CASCADE,
    username   text                  NOT NULL,
    is_primary boolean DEFAULT FALSE NOT NULL,
    UNIQUE (user_id, username)
);
COMMENT ON COLUMN users.overwatch_usernames.username IS 'Overwatch username';
COMMENT ON COLUMN users.overwatch_usernames.is_primary IS 'Only one primary username allowed';

CREATE UNIQUE INDEX IF NOT EXISTS unique_primary_per_user ON users.overwatch_usernames (user_id) WHERE is_primary;
CREATE INDEX IF NOT EXISTS idx_overwatch_username_trgm ON users.overwatch_usernames USING gin (username gin_trgm_ops);

CREATE TABLE IF NOT EXISTS core.completions
(
    id              int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    map_id          int            NOT NULL REFERENCES core.maps (id) ON DELETE CASCADE,
    user_id         bigint         NOT NULL REFERENCES core.users (id) ON DELETE CASCADE,
    time            numeric(10, 2) NOT NULL,
    screenshot      text           NOT NULL,
    video           text,
    verified        boolean        NOT NULL DEFAULT FALSE,
    verification_id bigint,
    message_id      bigint,
    completion      boolean        NOT NULL DEFAULT FALSE,
    inserted_at     timestamptz    NOT NULL DEFAULT now(),
    verified_by     bigint REFERENCES core.users (id),
    legacy          boolean                 DEFAULT FALSE,
    legacy_medal    text,
    wr_xp_check     boolean                 DEFAULT FALSE,
    reason          text,
    UNIQUE (map_id, user_id, inserted_at),
    UNIQUE (message_id)
);

CREATE INDEX IF NOT EXISTS idx_records_map_user_date ON core.completions (map_id, user_id, inserted_at DESC);
CREATE INDEX IF NOT EXISTS idx_records_inserted_at ON core.completions (inserted_at);
CREATE INDEX IF NOT EXISTS idx_records_user_date ON core.completions (user_id, inserted_at);
CREATE INDEX IF NOT EXISTS idx_records_map_id ON core.completions (map_id);
CREATE INDEX IF NOT EXISTS idx_completions_verified_nonlegacy_pair
ON core.completions (user_id, map_id)
WHERE verified = TRUE AND legacy = FALSE;
CREATE INDEX IF NOT EXISTS idx_completions_nonlegacy_best
  ON core.completions (user_id, map_id, time)
  WHERE legacy = FALSE;

COMMENT ON COLUMN core.completions.time IS 'How long it took the user to complete a the map';
COMMENT ON COLUMN core.completions.screenshot IS 'The URL to the uploaded screenshot';
COMMENT ON COLUMN core.completions.video IS 'The URL to the uploaded video (Usually YouTube)';
COMMENT ON COLUMN core.completions.verified IS 'If the record has been verified';
COMMENT ON COLUMN core.completions.verification_id IS 'The verification queue message ID snowflake in Discord';
COMMENT ON COLUMN core.completions.message_id IS 'The completions channel message ID snowflake in Discord';
COMMENT ON COLUMN core.completions.completion IS 'Whether the submission counts as a completion (submissions while in playtest are completions as well as submissions that lack a video)';
COMMENT ON COLUMN core.completions.verified_by IS 'The user ID who verified this submission';
COMMENT ON COLUMN core.completions.legacy IS 'Whether the submission is classified as legacy';
COMMENT ON COLUMN core.completions.legacy_medal IS 'The medal of the submission, if submission is classified as legacy';
COMMENT ON COLUMN core.completions.wr_xp_check IS 'A simple check to block multi World Record XP awards from being triggered';

CREATE OR REPLACE FUNCTION core.enforce_speed_rules_nonlegacy_only()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  best_time          numeric;
  best_is_completion boolean;
BEGIN
  -- Any write that sets the row to legacy = TRUE is always allowed.
  IF NEW.legacy IS TRUE THEN
    RETURN NEW;
  END IF;

  -- Serialize per (user,map) to avoid racey double-inserts/updates.
  PERFORM pg_advisory_xact_lock(NEW.user_id, NEW.map_id);

  -- Find the fastest existing NON-LEGACY row for this user/map (verified or not).
  SELECT c.time, c.completion
    INTO best_time, best_is_completion
  FROM core.completions c
  WHERE c.user_id = NEW.user_id
    AND c.map_id  = NEW.map_id
    AND c.legacy  = FALSE
  ORDER BY c.time ASC
  LIMIT 1;

  -- No non-legacy rows yet -> nothing to enforce.
  IF best_time IS NULL THEN
    RETURN NEW;
  END IF;

  -- Apply your rules
  IF NEW.completion IS TRUE THEN
    -- Completion must always beat the fastest non-legacy time.
    IF NEW.time >= best_time THEN
      RAISE EXCEPTION
        'completion=TRUE time % must be strictly faster than current best % (user %, map %)',
        NEW.time, best_time, NEW.user_id, NEW.map_id
        USING ERRCODE = '23514';
    END IF;

  ELSE
    -- completion=FALSE
    IF best_is_completion IS FALSE AND NEW.time >= best_time THEN
      RAISE EXCEPTION
        'completion=FALSE time % must be strictly faster than current best non-completion % (user %, map %)',
        NEW.time, best_time, NEW.user_id, NEW.map_id
        USING ERRCODE = '23514';
    END IF;
    -- If best is a completion=TRUE, slower is allowed.
  END IF;

  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_enforce_speed_rules_nonlegacy_only ON core.completions;

CREATE TRIGGER trg_enforce_speed_rules_nonlegacy_only
BEFORE INSERT OR UPDATE OF time, completion, user_id, map_id, legacy
ON core.completions
FOR EACH ROW
EXECUTE FUNCTION core.enforce_speed_rules_nonlegacy_only();


CREATE OR REPLACE FUNCTION maps.ratings_verify_on_completion()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- Set the matching rating to verified if it exists
  UPDATE maps.ratings r
     SET verified = TRUE
   WHERE r.map_id = NEW.map_id
     AND r.user_id = NEW.user_id
     AND r.verified IS DISTINCT FROM TRUE;

  RETURN NEW; -- result ignored for AFTER triggers
END
$$;

DROP TRIGGER IF EXISTS trg_ratings_verify_on_completion ON core.completions;

CREATE TRIGGER trg_ratings_verify_on_completion
AFTER INSERT OR UPDATE OF verified
ON core.completions
FOR EACH ROW
WHEN (
  NEW.verified IS TRUE
)
EXECUTE FUNCTION maps.ratings_verify_on_completion();

CREATE OR REPLACE FUNCTION core.sync_linked_code()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  target_current text;
BEGIN
  -- Avoid infinite ping-pong when we update the counterpart.
  IF pg_trigger_depth() > 1 THEN
    RETURN NEW;
  END IF;

  -- If unlinking (linked_code becomes NULL), clear the counterpart if it points back
  IF NEW.linked_code IS NULL THEN
    IF OLD.linked_code IS NOT NULL THEN
      UPDATE core.maps
         SET linked_code = NULL
       WHERE code = OLD.linked_code
         AND linked_code = OLD.code; -- only clear if it points back to us
    END IF;
    RETURN NEW;
  END IF;

  -- At this point NEW.linked_code is NOT NULL
  IF NEW.linked_code = NEW.code THEN
    RAISE EXCEPTION 'linked_code cannot equal code (%).', NEW.code;
  END IF;

  -- Ensure target exists
  PERFORM 1 FROM core.maps WHERE code = NEW.linked_code;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'linked_code % does not reference an existing code.', NEW.linked_code;
  END IF;

  -- Check the target's current link
  SELECT linked_code INTO target_current
  FROM core.maps
  WHERE code = NEW.linked_code
  FOR UPDATE;  -- serialize against concurrent writers on the target

  -- If target already linked to someone else (not us), forbid
  IF target_current IS NOT NULL AND target_current <> NEW.code THEN
    RAISE EXCEPTION
      'Code % is already linked to %, cannot also link to %.',
      NEW.linked_code, target_current, NEW.code;
  END IF;

  -- If target not linked yet, link it back to us
  IF target_current IS NULL THEN
    UPDATE core.maps
       SET linked_code = NEW.code
     WHERE code = NEW.linked_code
       AND linked_code IS NULL;  -- idempotent
  END IF;

  RETURN NEW;
END
$$;


DROP TRIGGER IF EXISTS trg_sync_linked_code ON core.maps;

CREATE TRIGGER trg_sync_linked_code
BEFORE INSERT OR UPDATE OF linked_code, code
ON core.maps
FOR EACH ROW
EXECUTE FUNCTION core.sync_linked_code();


CREATE TABLE IF NOT EXISTS completions.upvotes
(
    id                int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id           bigint         NOT NULL REFERENCES core.users (id) ON DELETE CASCADE,
    message_id        bigint REFERENCES core.completions (message_id) ON DELETE CASCADE,
    inserted_at       timestamptz DEFAULT now(),
    UNIQUE (message_id, user_id)
);

INSERT INTO maps.mechanics (name, position) VALUES ('Edge Climb', -1);
INSERT INTO maps.mechanics (name, position) VALUES ('Bhop', 0);
INSERT INTO maps.mechanics (name, position) VALUES ('Save Climb', 2);
INSERT INTO maps.mechanics (name, position) VALUES ('High Edge', 3);
INSERT INTO maps.mechanics (name, position) VALUES ('Distance Edge', 5);
INSERT INTO maps.mechanics (name, position) VALUES ('Quick Climb', 6);
INSERT INTO maps.mechanics (name, position) VALUES ('Slide', 7);
INSERT INTO maps.mechanics (name, position) VALUES ('Stall', 8);
INSERT INTO maps.mechanics (name, position) VALUES ('Dash', 9);
INSERT INTO maps.mechanics (name, position) VALUES ('Ultimate', 10);
INSERT INTO maps.mechanics (name, position) VALUES ('Emote Save Bhop', 11);
INSERT INTO maps.mechanics (name, position) VALUES ('Death Bhop', 12);
INSERT INTO maps.mechanics (name, position) VALUES ('Triple Jump', 13);
INSERT INTO maps.mechanics (name, position) VALUES ('Multi Climb', 14);
INSERT INTO maps.mechanics (name, position) VALUES ('Vertical Multi Climb', 15);
INSERT INTO maps.mechanics (name, position) VALUES ('Standing Create Bhop', 17);
INSERT INTO maps.mechanics (name, position) VALUES ('Crouch Edge', 1);
INSERT INTO maps.mechanics (name, position) VALUES ('Bhop First', 4);
INSERT INTO maps.mechanics (name, position) VALUES ('Create Bhop', 16);
INSERT INTO maps.mechanics (name, position) VALUES ('Save Double', 18);
INSERT INTO maps.restrictions (name, position) VALUES ('Bhop', -1);
INSERT INTO maps.restrictions (name, position) VALUES ('Dash Start', 0);
INSERT INTO maps.restrictions (name, position) VALUES ('Triple Jump', 1);
INSERT INTO maps.restrictions (name, position) VALUES ('Emote Save Bhop', 2);
INSERT INTO maps.restrictions (name, position) VALUES ('Death Bhop', 3);
INSERT INTO maps.restrictions (name, position) VALUES ('Multi Climb', 4);
INSERT INTO maps.restrictions (name, position) VALUES ('Standing Create Bhop', 6);
INSERT INTO maps.restrictions (name, position) VALUES ('Create Bhop', 5);
INSERT INTO maps.restrictions (name, position) VALUES ('Wall Climb', 7);
INSERT INTO maps.restrictions (name, position) VALUES ('Double Jump', 8);

CREATE TABLE IF NOT EXISTS maps.names (
    name text PRIMARY KEY
);


INSERT INTO maps.names (name) VALUES ('Circuit Royal');
INSERT INTO maps.names (name) VALUES ('Runasapi');
INSERT INTO maps.names (name) VALUES ('Practice Range');
INSERT INTO maps.names (name) VALUES ('Route 66');
INSERT INTO maps.names (name) VALUES ('Midtown');
INSERT INTO maps.names (name) VALUES ('Junkertown');
INSERT INTO maps.names (name) VALUES ('Colosseo');
INSERT INTO maps.names (name) VALUES ('Lijiang Tower (Lunar New Year)');
INSERT INTO maps.names (name) VALUES ('Dorado');
INSERT INTO maps.names (name) VALUES ('Throne of Anubis');
INSERT INTO maps.names (name) VALUES ('Castillo');
INSERT INTO maps.names (name) VALUES ('Blizzard World (Winter)');
INSERT INTO maps.names (name) VALUES ('Hollywood (Halloween)');
INSERT INTO maps.names (name) VALUES ('Black Forest (Winter)');
INSERT INTO maps.names (name) VALUES ('Petra');
INSERT INTO maps.names (name) VALUES ('Eichenwalde');
INSERT INTO maps.names (name) VALUES ('Workshop Island');
INSERT INTO maps.names (name) VALUES ('Chateau Guillard (Halloween)');
INSERT INTO maps.names (name) VALUES ('New Junk City');
INSERT INTO maps.names (name) VALUES ('Necropolis');
INSERT INTO maps.names (name) VALUES ('Kanezaka');
INSERT INTO maps.names (name) VALUES ('Havana');
INSERT INTO maps.names (name) VALUES ('Oasis');
INSERT INTO maps.names (name) VALUES ('Ayutthaya');
INSERT INTO maps.names (name) VALUES ('Volskaya Industries');
INSERT INTO maps.names (name) VALUES ('Hanamura');
INSERT INTO maps.names (name) VALUES ('Workshop Expanse');
INSERT INTO maps.names (name) VALUES ('Hanaoka');
INSERT INTO maps.names (name) VALUES ('Lijiang Tower');
INSERT INTO maps.names (name) VALUES ('Busan (Lunar New Year)');
INSERT INTO maps.names (name) VALUES ('Suravasa');
INSERT INTO maps.names (name) VALUES ('King''s Row');
INSERT INTO maps.names (name) VALUES ('King''s Row (Winter)');
INSERT INTO maps.names (name) VALUES ('Ecopoint: Antarctica');
INSERT INTO maps.names (name) VALUES ('Hanamura (Winter)');
INSERT INTO maps.names (name) VALUES ('Blizzard World');
INSERT INTO maps.names (name) VALUES ('Chateau Guillard');
INSERT INTO maps.names (name) VALUES ('Paraiso');
INSERT INTO maps.names (name) VALUES ('Workshop Green Screen');
INSERT INTO maps.names (name) VALUES ('Watchpoint: Gibraltar');
INSERT INTO maps.names (name) VALUES ('Shambali');
INSERT INTO maps.names (name) VALUES ('Eichenwalde (Halloween)');
INSERT INTO maps.names (name) VALUES ('Nepal');
INSERT INTO maps.names (name) VALUES ('Samoa');
INSERT INTO maps.names (name) VALUES ('Horizon Lunar Colony');
INSERT INTO maps.names (name) VALUES ('Paris');
INSERT INTO maps.names (name) VALUES ('Esperanca');
INSERT INTO maps.names (name) VALUES ('Black Forest');
INSERT INTO maps.names (name) VALUES ('Antarctic Peninsula');
INSERT INTO maps.names (name) VALUES ('Workshop Chamber');
INSERT INTO maps.names (name) VALUES ('Hollywood');
INSERT INTO maps.names (name) VALUES ('New Queen Street');
INSERT INTO maps.names (name) VALUES ('Rialto');
INSERT INTO maps.names (name) VALUES ('Busan');
INSERT INTO maps.names (name) VALUES ('Malevento');
INSERT INTO maps.names (name) VALUES ('Temple of Anubis');
INSERT INTO maps.names (name) VALUES ('Ilios');
INSERT INTO maps.names (name) VALUES ('Ecopoint: Antarctica (Winter)');
INSERT INTO maps.names (name) VALUES ('Numbani');
INSERT INTO maps.names (name) VALUES ('Adlersbrunn');
INSERT INTO maps.names (name) VALUES ('Aatlis');
INSERT INTO maps.names (name) VALUES ('Framework');
INSERT INTO maps.names (name) VALUES ('Tools');


CREATE TABLE IF NOT EXISTS public.newsfeed
(
    id        int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    timestamp timestamp with time zone DEFAULT now() NOT NULL,
    payload      json                                   NOT NULL
);
ALTER TABLE public.newsfeed
  ADD COLUMN IF NOT EXISTS event_type TEXT GENERATED ALWAYS AS (payload->>'type') STORED;

CREATE INDEX IF NOT EXISTS newsfeed_type_idx ON public.newsfeed (event_type);
CREATE INDEX IF NOT EXISTS newsfeed_id_desc_idx ON public.newsfeed (id DESC);

CREATE TABLE IF NOT EXISTS public.auth_users
(
    id       int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username text NOT NULL,
    info     text
);

CREATE TABLE IF NOT EXISTS public.api_tokens
(
    id      bigserial PRIMARY KEY,
    user_id bigint REFERENCES public.auth_users (id),
    api_key text UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS lootbox.key_types
(
    name text NOT NULL
        CONSTRAINT lootbox_box_types_pkey PRIMARY KEY
);
INSERT INTO lootbox.key_types (name) VALUES ('Classic'), ('Winter') ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS lootbox.active_key
(
    key text
        CONSTRAINT lootbox_active_key_lootbox_key_types_name_fk REFERENCES lootbox.key_types
);

INSERT INTO lootbox.active_key VALUES ('Classic');

CREATE TABLE IF NOT EXISTS lootbox.reward_types
(
    name     text NOT NULL,
    type     text NOT NULL,
    rarity   text,
    key_type text NOT NULL
        CONSTRAINT lootbox_reward_types_lootbox_fkey REFERENCES lootbox.key_types ON UPDATE CASCADE ON DELETE CASCADE,
    PRIMARY KEY (name, type, key_type)
);

CREATE TABLE IF NOT EXISTS lootbox.user_keys
(
    user_id   bigint REFERENCES core.users (id) ON UPDATE CASCADE ON DELETE CASCADE,
    key_type  text
        CONSTRAINT lootbox_user_keys_lootbox_fkey REFERENCES lootbox.key_types ON UPDATE CASCADE ON DELETE CASCADE,
    earned_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS lootbox.user_rewards
(
    user_id     bigint REFERENCES core.users (id) ON UPDATE CASCADE ON DELETE CASCADE,
    reward_name text,
    earned_at   timestamptz DEFAULT now(),
    reward_type text,
    key_type    text,
    CONSTRAINT lootbox_user_rewards_lootbox_reward_types_name_type_key_type_fk FOREIGN KEY (reward_name, reward_type, key_type) REFERENCES lootbox.reward_types ON UPDATE CASCADE ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lootbox.xp_multiplier
(
    value numeric(4, 2) NOT NULL DEFAULT 1
);

INSERT INTO lootbox.xp_multiplier (value) VALUES (1);

CREATE TABLE IF NOT EXISTS maps.mastery
(
    user_id  bigint NOT NULL
        CONSTRAINT map_mastery_users_user_id_fk REFERENCES core.users (id) ON UPDATE CASCADE,
    map_name text   NOT NULL
        CONSTRAINT map_mastery_map_names_user_id_fk REFERENCES maps.names (name) ON UPDATE CASCADE,
    medal    text   NOT NULL,
    CONSTRAINT map_mastery_pk PRIMARY KEY (user_id, map_name)
);

CREATE INDEX IF NOT EXISTS map_mastery_user_id_index ON maps.mastery (user_id);


CREATE TABLE IF NOT EXISTS lootbox.xp
(
    user_id bigint  NOT NULL PRIMARY KEY
        CONSTRAINT xp_users_user_id_fk REFERENCES core.users (id) ON UPDATE CASCADE,
    amount  integer NOT NULL
);



CREATE TABLE IF NOT EXISTS lootbox.main_tiers
(
    threshold integer NOT NULL,
    name      text    NOT NULL
);

CREATE TABLE IF NOT EXISTS lootbox.sub_tiers
(
    threshold integer NOT NULL,
    name      text    NOT NULL
);



INSERT INTO lootbox.main_tiers (threshold, name) VALUES (0, 'Newcomer');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (1, 'Initiate');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (2, 'Apprentice');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (3, 'Disciple');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (4, 'Enthusiast');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (5, 'Explorer');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (6, 'Visionary');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (7, 'Aficionado');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (8, 'Virtuoso');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (9, 'Savant');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (10, 'Prodigy');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (11, 'Shadow');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (12, 'Shinobi');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (13, 'Assassin');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (14, 'Ronin');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (15, 'Shogun');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (16, 'Dragon');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (17, 'Cyber Ninja');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (18, 'Legend');
INSERT INTO lootbox.main_tiers (threshold, name) VALUES (19, 'Mythic');

INSERT INTO lootbox.sub_tiers (threshold, name) VALUES (0, 'I');
INSERT INTO lootbox.sub_tiers (threshold, name) VALUES (1, 'II');
INSERT INTO lootbox.sub_tiers (threshold, name) VALUES (2, 'III');
INSERT INTO lootbox.sub_tiers (threshold, name) VALUES (3, 'IV');
INSERT INTO lootbox.sub_tiers (threshold, name) VALUES (4, 'V');

CREATE TABLE IF NOT EXISTS public.change_requests
(
    thread_id           bigint                                NOT NULL PRIMARY KEY,
    code                text                                  NOT NULL REFERENCES core.maps (code) ON UPDATE CASCADE ON DELETE CASCADE,
    user_id             bigint                                NOT NULL REFERENCES core.users ON UPDATE CASCADE ON DELETE CASCADE,
    resolved            boolean     DEFAULT FALSE,
    created_at          timestamptz DEFAULT current_timestamp NOT NULL,
    change_request_type text                                  NOT NULL,
    content             text                                  NOT NULL,
    creator_mentions    text                                  NOT NULL,
    alerted             boolean     DEFAULT FALSE
);


CREATE TABLE IF NOT EXISTS rank_card.avatar
(
    user_id bigint                           NOT NULL
        CONSTRAINT rank_card_avatar_pk PRIMARY KEY
        CONSTRAINT rank_card_avatar_users_user_id_fk REFERENCES core.users,
    skin    text DEFAULT 'Overwatch 1'::text NOT NULL,
    pose    text DEFAULT 'Heroic'::text      NOT NULL
);

CREATE TABLE IF NOT EXISTS rank_card.background
(
    name     text   NOT NULL,
    user_id  bigint NOT NULL
        CONSTRAINT rank_card_background_pk PRIMARY KEY
        CONSTRAINT rank_card_background_users_user_id_fk REFERENCES core.users,
    key_type text
);

CREATE TABLE IF NOT EXISTS rank_card.badges
(
    user_id     bigint NOT NULL
        CONSTRAINT rank_card_badges_pk PRIMARY KEY
        CONSTRAINT rank_card_badges_users_user_id_fk REFERENCES core.users,
    badge_name1 text,
    badge_type1 text,
    badge_name2 text,
    badge_type2 text,
    badge_name3 text,
    badge_type3 text,
    badge_name4 text,
    badge_type4 text,
    badge_name5 text,
    badge_type5 text,
    badge_name6 text,
    badge_type6 text
);

CREATE TABLE users.suspicious_flags
(
    id              int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id         bigint REFERENCES core.users (id) ON DELETE CASCADE,
    completion_id   bigint REFERENCES core.completions (id) ON DELETE CASCADE,
    context         text,
    flag_type       text,
    flagged_by      bigint REFERENCES core.users (id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS suspicious_flags_user_id_idx
  ON users.suspicious_flags (user_id);

COMMENT ON COLUMN users.suspicious_flags.context IS 'A description of why the flag has been given.';
COMMENT ON COLUMN users.suspicious_flags.flag_type IS 'The type of flag given to the completion.';

CREATE TABLE IF NOT EXISTS public.analytics (
    command_name text        NOT NULL,
    user_id      bigint      NOT NULL,
    created_at   timestamptz NOT NULL,
    namespace    jsonb,
    CONSTRAINT analytics_pkey PRIMARY KEY (command_name, user_id, created_at)
);

-- Indexes to support common queries
CREATE INDEX IF NOT EXISTS analytics_date_idx  ON public.analytics (created_at);
CREATE INDEX IF NOT EXISTS analytics_command_name_idx ON public.analytics (command_name);
CREATE INDEX IF NOT EXISTS analytics_command_name_date_idx ON public.analytics (command_name, created_at);
COMMIT;
