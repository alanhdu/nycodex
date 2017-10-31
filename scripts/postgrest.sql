DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='web_anon') THEN
        CREATE ROLE web_anon NOLOGIN;
    END IF;
END
$$;
GRANT web_anon TO postgres;
GRANT USAGE ON SCHEMA api TO web_anon;
GRANT SELECT ON ALL TABLES IN SCHEMA api TO web_anon;
