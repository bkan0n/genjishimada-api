INSERT INTO public.auth_users (username, info) VALUES ('testing', 'testing');
INSERT INTO public.api_tokens (user_id, api_key) VALUES (1, 'testing');


-- Seed base user data

INSERT INTO core.users (id, nickname, global_name, coins)
VALUES
  (1, 'Test Nick (FakeUser1)', 'Test Global (FakeUser1)', 0),
  (2, 'Test Creator (FakeUser2)', 'Test Creator (FakeUser2)', 0),
  (23, 'Test Notifications1', 'Test Notifications1', 0),
  (24, 'Test Notifications2', 'Test Notifications2', 0),
  (25, 'Test Notifications3', 'Test Notifications3', 0),
  (100000000000000000, 'ShadowSlayerNick', 'ShadowSlayerGlobal', 50),
  (100000000000000001, 'PixelCreatorNick',    'PixelMageGlobal',    75),
  (100000000000000002, 'WangCreatorNick',   'WangCreatorGlobal',   20),
  (100000000000000003, 'BangCreatorNick',   'BangCreatorGlobal',   20),
  (100000000000000004, 'TangKnightNick',    'TangKnightGlobal',   20),
  (100000000000000005, 'PreUpdateNick1',    'PreUpdateGlobal1',   20),
  (100000000000000007, 'PreUpdateNick2',    'PreUpdateGlobal2',   20),
  (100000000000000008, 'PreUpdateNick3',    'PreUpdateGlobal3',   20),
  (100000000000000006, 'LangKnightNick',    'LangKnightGlobal',   20),
  (50, 'KeepsKeys',    'KeepsKeys',   6969),
  (51, 'LoseKeys',    'LoseKeys',   696969),
  (52, 'GrantKeys',    'GrantKeys',   696969),
  (53, 'GuideMaker',    'GuideMaker',   696969),
  (54, 'GuideMaker2',    'GuideMaker2',   696969);

INSERT INTO users.notification_settings (user_id, flags) VALUES (23, 231);

INSERT INTO users.overwatch_usernames (user_id, username, is_primary)
  VALUES
    (100000000000000000, 'ShadowSlayer#1001',  TRUE),
    (100000000000000000, 'ShadowSlayerAlt#1001', FALSE);

  -- User 1002 has one primary username
INSERT INTO users.overwatch_usernames (user_id, username, is_primary)
  VALUES
    (100000000000000001, 'PixelMage#2002', TRUE);

  -- User 1003 has two usernames, one primary
INSERT INTO users.overwatch_usernames (user_id, username, is_primary)
  VALUES
    (100000000000000002, 'NovaKnightOW1',    TRUE),
    (100000000000000002, 'NovaKnightShadowOW2', FALSE),
    (100000000000000002, 'NovaKnightShadowOW3', FALSE);



INSERT INTO core.maps(code, map_name, category, checkpoints, description, difficulty, raw_difficulty) VALUES
('1EASY', 'Hanamura', 'Classic', 69,  'Test Map 1 ', 'Easy -',      0.0),
('2EASY', 'Hanamura', 'Classic', 69,  'Test Map 2 ', 'Easy',        0.20),
('4EASY', 'Hanamura', 'Classic', 69,  'Test Map 3 ', 'Easy',        0.20),
('5EASY', 'Hanamura', 'Classic', 69,  'Test Map 4 ', 'Easy',        0.20),
('6EASY', 'Hanamura', 'Classic', 69,  'Test Map 5 ', 'Easy',        0.20),
('7EASY', 'Hanamura', 'Classic', 69,  'Test Map 6 ', 'Easy',        0.20),
('8EASY', 'Hanamura', 'Classic', 69,  'Test Map 7 ', 'Easy',        0.20),
('9EASY', 'Hanamura', 'Classic', 69,  'Test Map 8 ', 'Easy',        0.20),
('10EASY', 'Hanamura', 'Classic', 69, 'Test Map 9 ', 'Easy',        0.20),
('3EASY', 'Hanamura', 'Classic', 69,  'Test Map 10', 'Easy +',        2.0),
('1MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 11', 'Medium -',     2.5),
('2MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 12', 'Medium',     3.0),
('4MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 13', 'Medium',     3.0),
('5MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 14', 'Medium',     3.0),
('6MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 15', 'Medium',     3.0),
('7MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 16', 'Medium',     3.0),
('8MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 17', 'Medium',     3.0),
('9MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 18', 'Medium',     3.0),
('10MEDI', 'Hanamura', 'Classic', 69, 'Test Map 19', 'Medium',     3.0),
('3MEDIU', 'Hanamura', 'Classic', 69, 'Test Map 20', 'Medium +',     4.0),
('1HARD', 'Hanamura', 'Classic', 69,  'Test Map 21', 'Hard -',        4.7),
('2HARD', 'Hanamura', 'Classic', 69,  'Test Map 22', 'Hard',        5.0),
('4HARD', 'Hanamura', 'Classic', 69,  'Test Map 23', 'Hard',        5.0),
('5HARD', 'Hanamura', 'Classic', 69,  'Test Map 24', 'Hard',        5.0),
('6HARD', 'Hanamura', 'Classic', 69,  'Test Map 25', 'Hard',        5.0),
('7HARD', 'Hanamura', 'Classic', 69,  'Test Map 26', 'Hard',        5.0),
('3HARD', 'Hanamura', 'Classic', 69,  'Test Map 27', 'Hard +',        5.5),
('1VHARD', 'Hanamura', 'Classic', 69, 'Test Map 28', 'Very Hard -', 6.0),
('2VHARD', 'Hanamura', 'Classic', 69, 'Test Map 29', 'Very Hard', 7.0),
('4VHARD', 'Hanamura', 'Classic', 69, 'Test Map 30', 'Very Hard', 7.0),
('5VHARD', 'Hanamura', 'Classic', 69, 'Test Map 31', 'Very Hard', 7.0),
('3VHARD', 'Hanamura', 'Classic', 69, 'Test Map 32', 'Very Hard +', 7.5),
('1EXTRE', 'Hanamura', 'Classic', 69, 'Test Map 33', 'Extreme -',   8.0),
('2EXTRE', 'Hanamura', 'Classic', 69, 'Test Map 34', 'Extreme',   8.5),
('3EXTRE', 'Hanamura', 'Classic', 69, 'Test Map 35', 'Extreme +',   9.0),
('1HELL', 'Hanamura', 'Classic', 69,  'Test Map 36', 'Hell',       9.5),
('2HELL', 'Hanamura', 'Classic', 69,  'Test Map 37', 'Hell',       9.6),
('3HELL', 'Hanamura', 'Classic', 69,  'Test Map 38', 'Hell',       10.0),
('GUIDE', 'Hanamura', 'Classic', 69,  'Map with 1 guide (editing tests)', 'Easy -',      0.0),
('1GUIDE', 'Hanamura', 'Classic', 69,  'Map with 1 guide (delete tests)', 'Easy -',      0.0),
('2GUIDE', 'Hanamura', 'Classic', 69,  'Map with 2 guides ', 'Easy -',      0.0),
('3GUIDE', 'Hanamura', 'Classic', 69,  'Map with 1 guide (edit guide) ', 'Easy -',      0.0);


INSERT INTO maps.creators (user_id, map_id, is_primary) VALUES
(100000000000000001, 1, TRUE),
(100000000000000001, 2, TRUE),
(100000000000000001, 3, TRUE),
(100000000000000001, 4, TRUE),
(100000000000000001, 5, TRUE),
(100000000000000001, 6, TRUE),
(100000000000000001, 7, TRUE),
(100000000000000001, 8, TRUE),
(100000000000000001, 9, TRUE),
(100000000000000001, 10, TRUE),
(100000000000000002, 11, TRUE),
(100000000000000002, 12, TRUE),
(100000000000000002, 13, TRUE),
(100000000000000002, 14, TRUE),
(100000000000000002, 15, TRUE),
(100000000000000002, 16, TRUE),
(100000000000000002, 17, TRUE),
(100000000000000002, 18, TRUE),
(100000000000000002, 19, TRUE),
(100000000000000002, 20, TRUE),
(100000000000000003, 21, TRUE),
(100000000000000003, 22, TRUE),
(100000000000000003, 23, TRUE),
(100000000000000003, 24, TRUE),
(100000000000000003, 25, TRUE),
(100000000000000003, 26, TRUE),
(100000000000000003, 27, TRUE),
(100000000000000003, 28, TRUE),
(100000000000000003, 29, TRUE),
(100000000000000003, 30, TRUE),
(2, 31, TRUE),
(2, 32, TRUE),
(2, 33, TRUE),
(2, 34, TRUE),
(2, 35, TRUE),
(2, 36, TRUE),
(2, 37, TRUE),
(2, 38, TRUE),
(100000000000000003, 39, TRUE),
(100000000000000003, 40, TRUE),
(100000000000000003, 1,  FALSE),
(100000000000000003, 2,  FALSE),
(100000000000000003, 3,  FALSE),
(100000000000000003, 4,  FALSE),
(100000000000000003, 5,  FALSE),
(100000000000000003, 6,  FALSE),
(100000000000000003, 7,  FALSE),
(100000000000000003, 8,  FALSE),
(100000000000000003, 9,  FALSE),
(100000000000000003, 10, FALSE),
(100000000000000002, 1, FALSE),
(100000000000000002, 2, FALSE),
(100000000000000002, 3, FALSE),
(100000000000000002, 4, FALSE),
(100000000000000002, 5, FALSE),
(100000000000000002, 6, FALSE),
(100000000000000002, 7, FALSE),
(100000000000000002, 8, FALSE),
(100000000000000002, 9, FALSE),
(100000000000000002, 10, FALSE);

INSERT INTO maps.guides (map_id, user_id, url) VALUES
(40, 53, 'https://www.youtube.com/watch?v=FJs41oeAnHU'),
(41, 53, 'https://www.youtube.com/watch?v=FJs41oeAnHU'),
(41, 54, 'https://www.youtube.com/watch?v=GU8htjxY6ro'),
(42, 54, 'https://www.youtube.com/watch?v=GU8htjxY6ro');


-- Lootbox --

INSERT INTO lootbox.user_keys (user_id, key_type) VALUES (50, 'Classic');
INSERT INTO lootbox.user_keys (user_id, key_type) VALUES (50, 'Winter');
INSERT INTO lootbox.user_keys (user_id, key_type) VALUES (51, 'Classic');
INSERT INTO lootbox.user_keys (user_id, key_type) VALUES (51, 'Winter');
