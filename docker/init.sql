-- Local development schema
-- Simplified version of 05.3-ddl.md without Supabase-specific extensions:
--   - No auth.users FK (Supabase Auth not present locally)
--   - Standard trigger function instead of extensions.moddatetime
--   - No RLS policies (auth.uid() is a Supabase function)
--
-- For production, apply 05-database/05.3-ddl.md against a Supabase project.

-- ── Trigger function ───────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

-- ── Pipeline tables ────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.users (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER set_users_updated_at
    BEFORE UPDATE ON public.users
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


CREATE TABLE IF NOT EXISTS public.batches (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id           UUID NOT NULL REFERENCES public.users (id) ON DELETE CASCADE,
    status            TEXT NOT NULL DEFAULT 'pending_upload',
    total_documents   INTEGER NOT NULL,
    queued_count      INTEGER NOT NULL DEFAULT 0,
    completed_count   INTEGER NOT NULL DEFAULT 0,
    failed_count      INTEGER NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT batches_status_check CHECK (
        status IN ('pending_upload', 'processing', 'completed', 'partial_failure', 'failed')
    ),
    CONSTRAINT batches_total_documents_positive    CHECK (total_documents >= 1),
    CONSTRAINT batches_queued_count_non_negative   CHECK (queued_count >= 0),
    CONSTRAINT batches_completed_count_non_negative CHECK (completed_count >= 0),
    CONSTRAINT batches_failed_count_non_negative   CHECK (failed_count >= 0),
    CONSTRAINT batches_counters_within_total       CHECK (completed_count + failed_count <= total_documents),
    CONSTRAINT batches_queued_within_total         CHECK (queued_count <= total_documents)
);

CREATE INDEX IF NOT EXISTS idx_batches_user_id ON public.batches (user_id);

CREATE TRIGGER set_batches_updated_at
    BEFORE UPDATE ON public.batches
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();


CREATE TABLE IF NOT EXISTS public.documents (
    id                    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id              UUID NOT NULL REFERENCES public.batches (id) ON DELETE CASCADE,
    user_id               UUID NOT NULL REFERENCES public.users (id) ON DELETE CASCADE,
    original_filename     TEXT NOT NULL,
    s3_key                TEXT NOT NULL,
    file_size             INTEGER NOT NULL,
    doc_type              TEXT NOT NULL,
    status                TEXT NOT NULL DEFAULT 'pending_upload',
    extracted_data        JSONB,
    error_reason          TEXT,
    retry_count           INTEGER NOT NULL DEFAULT 0,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    processing_started_at TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    promoted_at           TIMESTAMPTZ,

    CONSTRAINT documents_doc_type_check CHECK (doc_type IN ('xml', 'pdf', 'img')),
    CONSTRAINT documents_status_check CHECK (
        status IN ('pending_upload', 'upload_confirmed', 'queued', 'processing', 'completed', 'failed')
    ),
    CONSTRAINT documents_file_size_positive          CHECK (file_size > 0),
    CONSTRAINT documents_retry_count_non_negative    CHECK (retry_count >= 0),
    CONSTRAINT documents_extracted_data_is_object    CHECK (extracted_data IS NULL OR jsonb_typeof(extracted_data) = 'object'),
    CONSTRAINT documents_s3_key_not_empty            CHECK (s3_key <> ''),
    CONSTRAINT documents_filename_not_empty          CHECK (original_filename <> '')
);

CREATE INDEX IF NOT EXISTS idx_documents_batch_id_status ON public.documents (batch_id, status);
CREATE INDEX IF NOT EXISTS idx_documents_user_id         ON public.documents (user_id);
CREATE INDEX IF NOT EXISTS idx_documents_promotable      ON public.documents (completed_at)
    WHERE status = 'completed' AND promoted_at IS NULL;

CREATE TRIGGER set_documents_updated_at
    BEFORE UPDATE ON public.documents
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ── Analytics tables ───────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS public.entidades (
    documento   TEXT PRIMARY KEY,
    nome        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);


CREATE TABLE IF NOT EXISTS public.invoices (
    chave_acesso              TEXT PRIMARY KEY,
    document_id               UUID NOT NULL REFERENCES public.documents (id),
    user_id                   UUID NOT NULL REFERENCES public.users (id) ON DELETE CASCADE,
    cnpj_emitente             TEXT NOT NULL REFERENCES public.entidades (documento),
    documento_destinatario    TEXT NOT NULL REFERENCES public.entidades (documento),
    data_emissao              DATE NOT NULL,
    numero_nota               TEXT NOT NULL,
    serie                     TEXT NOT NULL,
    valor_total               NUMERIC(15, 2) NOT NULL CHECK (valor_total > 0),
    doc_type                  TEXT NOT NULL CHECK (doc_type IN ('xml', 'pdf', 'img')),
    confidence                SMALLINT CHECK (confidence BETWEEN 0 AND 100),
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_invoices_user_id                ON public.invoices (user_id);
CREATE INDEX IF NOT EXISTS idx_invoices_cnpj_emitente          ON public.invoices (cnpj_emitente);
CREATE INDEX IF NOT EXISTS idx_invoices_documento_destinatario ON public.invoices (documento_destinatario);
CREATE INDEX IF NOT EXISTS idx_invoices_data_emissao           ON public.invoices (data_emissao);
CREATE INDEX IF NOT EXISTS idx_invoices_document_id            ON public.invoices (document_id);


CREATE TABLE IF NOT EXISTS public.invoice_items (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chave_acesso     TEXT NOT NULL REFERENCES public.invoices (chave_acesso) ON DELETE CASCADE,
    user_id          UUID NOT NULL REFERENCES public.users (id) ON DELETE CASCADE,
    numero_item      INTEGER NOT NULL,
    codigo_produto   TEXT NOT NULL,
    descricao        TEXT NOT NULL,
    ncm              TEXT NOT NULL,
    cfop             TEXT NOT NULL,
    unidade          TEXT NOT NULL,
    quantidade       NUMERIC(15, 4),
    valor_unitario   NUMERIC(15, 4),
    valor_total_item NUMERIC(15, 2),

    CONSTRAINT invoice_items_numero_item_unique UNIQUE (chave_acesso, numero_item)
);

CREATE INDEX IF NOT EXISTS idx_invoice_items_ncm            ON public.invoice_items (ncm);
CREATE INDEX IF NOT EXISTS idx_invoice_items_codigo_produto ON public.invoice_items (codigo_produto);
CREATE INDEX IF NOT EXISTS idx_invoice_items_user_id        ON public.invoice_items (user_id);
