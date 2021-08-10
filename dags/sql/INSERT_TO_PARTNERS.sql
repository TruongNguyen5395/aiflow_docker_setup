INSERT INTO partners (partner_name, partner_status)
VALUES ('A', FALSE)
ON CONFLICT(partner_name)
DO NOTHING;
INSERT INTO partners (partner_name, partner_status)
VALUES ('B', FALSE)
ON CONFLICT(partner_name)
DO NOTHING;
INSERT INTO partners (partner_name, partner_status)
VALUES ('C', FALSE)
ON CONFLICT(partner_name)
DO NOTHING;
INSERT INTO partners (partner_name, partner_status)
VALUES ('D', FALSE)
ON CONFLICT(partner_name)
DO NOTHING;
