defmodule Realtime.RLS.Replications do
  import Realtime.RLS.Repo

  def prepare_replication(slot_name, temporary_slot) do
    query(
      "select
        case
          when not exists (
            select 1
            from pg_replication_slots
            where slot_name = $1
          )
          then (
            select 1 from pg_create_logical_replication_slot($1, 'wal2json', $2)
          )
          else 1
        end;",
      [slot_name, temporary_slot]
    )
    |> case do
      {:ok, %Postgrex.Result{rows: [[1]]}} ->
        {:ok, slot_name}

      {_, error} ->
        {:error, error}
    end
  end

  def list_changes(slot_name, publication, max_changes, max_record_bytes) do
    query(
      "with pub as (
        select
          concat_ws(
            ',',
            case when bool_or(pubinsert) then 'insert' else null end,
            case when bool_or(pubupdate) then 'update' else null end,
            case when bool_or(pubdelete) then 'delete' else null end
          ) as w2j_actions,
          coalesce(
            string_agg(
              realtime.quote_wal2json(format('%I.%I', schemaname, tablename)::regclass),
              ','
            ) filter (where ppt.tablename is not null),
            ''
          ) w2j_add_tables
        from
          pg_publication pp
          left join pg_publication_tables ppt
            on pp.pubname = ppt.pubname
        where
          pp.pubname = $1
        group by
          pp.pubname
        limit 1
      ),
      w2j as (
        select
          x.*, pub.w2j_add_tables
        from
          pub,
          pg_logical_slot_get_changes(
            $2, null, $3,
            'include-pk', 'true',
            'include-transaction', 'false',
            'include-timestamp', 'true',
            'include-type-oids', 'true',
            'format-version', '2',
            'actions', pub.w2j_actions,
            'add-tables', pub.w2j_add_tables
          ) x
      )
      select
        xyz.wal,
        xyz.is_rls_enabled,
        xyz.subscription_ids,
        xyz.errors
      from
        w2j,
        realtime.apply_rls(
          wal := w2j.data::jsonb,
          max_record_bytes := $4
        ) xyz(wal, is_rls_enabled, subscription_ids, errors)
      where
        w2j.w2j_add_tables <> ''
        and xyz.subscription_ids[1] is not null",
      [
        publication,
        slot_name,
        max_changes,
        max_record_bytes
      ]
    )
  end
end
