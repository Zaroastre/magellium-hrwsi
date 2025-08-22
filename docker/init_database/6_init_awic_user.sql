-- For the AWIC requirements, specific user creation
-- with read access only granted to the hrwsi.products table
CREATE USER awic_user WITH PASSWORD '*eNF2ft0o*g$PD&ytGn6';

GRANT USAGE ON SCHEMA hrwsi TO awic_user;
GRANT SELECT ON TABLE hrwsi.products TO awic_user;