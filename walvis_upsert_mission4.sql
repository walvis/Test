DROP FUNCTION IF EXISTS walvis_upsert_mission4(int[], int[]);

-- Returns a set of op,cartodb_id values where op means:
--
--  deleted: -1
--  updated: 0
--  inserted: 1
--
CREATE OR REPLACE FUNCTION walvis_upsert_mission4(
  cartodb_ids integer[],
  status integer[])
  RETURNS TABLE(op int, cartodb_id int)

LANGUAGE plpgsql SECURITY DEFINER
RETURNS NULL ON NULL INPUT
AS $$
DECLARE
sql text;
BEGIN

sql := 'WITH n(cartodb_id,status) AS (VALUES ';

--Iterate over the values
FOR i in 1 .. array_upper(geojsons, 1)
LOOP
  IF i > 1 THEN sql := sql || ','; END IF;
  sql :=sql || '('||cartodb_ids[i]||','
			|| status[i]||')';
END LOOP;

sql := sql || '), do_update AS ('
      || 'UPDATE mission_ks_parcels p '
      || 'SET status=n.status FROM n WHERE p.cartodb_id = n.cartodb_id '
      || 'AND n.status IS NOT NULL '
      || 'RETURNING p.cartodb_id ), do_delete AS ('
      || 'DELETE FROM mission_ks_parcels p WHERE p.cartodb_id IN ('
      || 'SELECT n.cartodb_id FROM n WHERE cartodb_id >= 0) RETURNING p.cartodb_id ), do_insert AS ('
      || 'INSERT INTO mission_ks_parcels (status)'
      || 'SELECT n.status FROM n WHERE n.cartodb_id < 0 AND '
      || ' n.status IS NOT NULL RETURNING cartodb_id ) '
      || 'SELECT 0,cartodb_id FROM do_update UNION ALL '
      || 'SELECT 1,cartodb_id FROM do_insert UNION ALL '
      || 'SELECT -1,cartodb_id FROM do_delete';

RAISE DEBUG '%', sql;

RETURN QUERY EXECUTE sql;

END;
$$;

--Grant access to the public user
GRANT EXECUTE ON FUNCTION walvis_upsert_mission4(integer[],integer[]) TO publicuser;