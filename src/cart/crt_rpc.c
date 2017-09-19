/* Copyright (C) 2016-2017 Intel Corporation
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted for any purpose (including commercial purposes)
 * provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions, and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions, and the following disclaimer in the
 *    documentation and/or materials provided with the distribution.
 *
 * 3. In addition, redistributions of modified forms of the source or binary
 *    code must carry prominent notices stating that the original code was
 *    changed and the date of the change.
 *
 *  4. All publications or advertising materials mentioning features or use of
 *     this software are asked, but not required, to acknowledge that it was
 *     developed by Intel Corporation and credit the contributors.
 *
 * 5. Neither the name of Intel Corporation, nor the name of any Contributor
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
 * THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This file is part of CaRT. It implements the main RPC routines.
 */
#define D_LOGFAC	DD_FAC(rpc)

#include "crt_internal.h"

#include <abt.h>

/* CRT internal RPC format definitions */

/* group create */
static struct crt_msg_field *crt_grp_create_in_fields[] = {
	&CMF_GRP_ID,		/* gc_grp_id */
	&CMF_UINT64,		/* gc_int_grpid */
	&CMF_RANK_LIST,		/* gc_membs */
	&CMF_RANK,		/* gc_initiate_rank */
};

static struct crt_msg_field *crt_grp_create_out_fields[] = {
	&CMF_RANK_LIST,		/* gc_failed_ranks */
	&CMF_RANK,		/* gc_rank */
	&CMF_INT,		/* gc_rc */
};

static struct crt_req_format CQF_CRT_GRP_CREATE =
	DEFINE_CRT_REQ_FMT("CRT_GRP_CREATE", crt_grp_create_in_fields,
			   crt_grp_create_out_fields);

/* group destroy */
static struct crt_msg_field *crt_grp_destroy_in_fields[] = {
	&CMF_GRP_ID,		/* gd_grp_id */
	&CMF_RANK,		/* gd_initiate_rank */
};

static struct crt_msg_field *crt_grp_destroy_out_fields[] = {
	&CMF_RANK_LIST,		/* gd_failed_ranks */
	&CMF_RANK,		/* gd_rank */
	&CMF_INT,		/* gd_rc */
};

static struct crt_req_format CQF_CRT_GRP_DESTROY =
	DEFINE_CRT_REQ_FMT("CRT_GRP_DESTROY", crt_grp_destroy_in_fields,
			   crt_grp_destroy_out_fields);

/* uri lookup */
static struct crt_msg_field *crt_uri_lookup_in_fields[] = {
	&CMF_GRP_ID,		/* ul_grp_id */
	&CMF_RANK,		/* ul_rank */
};

static struct crt_msg_field *crt_uri_lookup_out_fields[] = {
	&CMF_PHY_ADDR,		/* ul_uri */
	&CMF_INT,		/* ul_rc */
};

static struct crt_req_format CQF_CRT_URI_LOOKUP =
	DEFINE_CRT_REQ_FMT("CRT_URI_LOOKUP", crt_uri_lookup_in_fields,
			   crt_uri_lookup_out_fields);

/* for self-test service */
static struct crt_msg_field *crt_st_send_id_field[] = {
	&CMF_UINT64,
};

static struct crt_msg_field *crt_st_send_id_iov_field[] = {
	&CMF_UINT64,
	&CMF_IOVEC,
};

static struct crt_msg_field *crt_st_send_id_iov_bulk_field[] = {
	&CMF_UINT64,
	&CMF_IOVEC,
	&CMF_BULK,
};

static struct crt_msg_field *crt_st_send_id_bulk_field[] = {
	&CMF_UINT64,
	&CMF_BULK,
};

static struct crt_msg_field *crt_st_reply_iov_field[] = {
	&CMF_IOVEC,
};

static struct crt_msg_field *crt_st_open_session_field[] = {
	&CMF_UINT32,
	&CMF_UINT32,
	&CMF_UINT32,
	&CMF_UINT32,
};

static struct crt_msg_field *crt_st_session_id_field[] = {
	&CMF_UINT64,
};

static struct crt_msg_field *crt_st_start_field[] = {
	&CMF_GRP_ID,
	&CMF_IOVEC,
	&CMF_UINT32,
	&CMF_UINT32,
	&CMF_UINT32,
	&CMF_UINT32,
	&CMF_UINT32,
};

static struct crt_msg_field *crt_st_start_reply_field[] = {
	&CMF_INT,
};

static struct crt_msg_field *crt_st_status_req_field[] = {
	&CMF_BULK,
};

static struct crt_msg_field *crt_st_status_req_reply_field[] = {
	&CMF_UINT64,
	&CMF_UINT32,
	&CMF_INT,
};

static struct crt_req_format CQF_CRT_SELF_TEST_SEND_EMPTY_REPLY_IOV =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_SEND_EMPTY_REPLY_IOV",
			   crt_st_send_id_field,
			   crt_st_reply_iov_field);

static struct crt_req_format CQF_CRT_SELF_TEST_SEND_IOV_REPLY_EMPTY =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_SEND_IOV_REPLY_EMPTY",
			   crt_st_send_id_iov_field,
			   NULL);

static struct crt_req_format CQF_CRT_SELF_TEST_BOTH_IOV =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_BOTH_IOV",
			   crt_st_send_id_iov_field,
			   crt_st_reply_iov_field);

static struct crt_req_format CQF_CRT_SELF_TEST_SEND_IOV_REPLY_BULK =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_SEND_IOV_REPLY_BULK",
			   crt_st_send_id_iov_bulk_field,
			   NULL);

static struct crt_req_format CQF_CRT_SELF_TEST_SEND_BULK_REPLY_IOV =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_SEND_BULK_REPLY_IOV",
			   crt_st_send_id_bulk_field,
			   crt_st_reply_iov_field);

static struct crt_req_format CQF_CRT_SELF_TEST_BOTH_BULK =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_BOTH_BULK",
			   crt_st_send_id_bulk_field,
			   NULL);

static struct crt_req_format CQF_CRT_SELF_TEST_OPEN_SESSION =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_OPEN_SESSION",
			   crt_st_open_session_field,
			   crt_st_session_id_field);

static struct crt_req_format CQF_CRT_SELF_TEST_CLOSE_SESSION =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_CLOSE_SESSION",
			   crt_st_session_id_field,
			   NULL);

static struct crt_req_format CQF_CRT_SELF_TEST_START =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_START",
			   crt_st_start_field,
			   crt_st_start_reply_field);

static struct crt_req_format CQF_CRT_SELF_TEST_STATUS_REQ =
	DEFINE_CRT_REQ_FMT("CRT_SELF_TEST_STATUS_REQ",
			   crt_st_status_req_field,
			   crt_st_status_req_reply_field);



static struct crt_msg_field *crt_iv_fetch_in_fields[] = {
	&CMF_IOVEC, /* crt_ivns_id */
	&CMF_IOVEC, /* key */
	&CMF_BULK, /* iv_value_bulk */
	&CMF_INT, /* class_id */
	&CMF_RANK, /* root_rank */
};

static struct crt_msg_field *crt_iv_fetch_out_fields[] = {
	&CMF_INT, /* TODO: int for now */
};

static struct crt_msg_field *crt_iv_update_in_fields[] = {
	&CMF_IOVEC, /* crt_ivns_id */
	&CMF_IOVEC, /* key */
	&CMF_IOVEC, /* iov_sync_type */
	&CMF_BULK, /* iv_value_bulk */
	&CMF_RANK, /* root_node */
	&CMF_RANK, /* caller_node */
	&CMF_UINT32, /* class_id */
	&CMF_UINT32, /* padding */
};

static struct crt_msg_field *crt_iv_update_out_fields[] = {
	&CMF_UINT64, /* rc */
};


static struct crt_msg_field *crt_iv_sync_in_fields[] = {
	&CMF_IOVEC, /* crt_ivns_id */
	&CMF_IOVEC, /* key */
	&CMF_IOVEC, /* iov_sync_type */
	&CMF_BULK, /* iv_value_bulk */
	&CMF_UINT32, /* class_id */
};

static struct crt_msg_field *crt_iv_sync_out_fields[] = {
	&CMF_INT,
};

static struct crt_corpc_ops crt_iv_sync_co_ops = {
	.co_aggregate = crt_iv_sync_corpc_aggregate,
};


static struct crt_req_format CQF_CRT_IV_SYNC =
	DEFINE_CRT_REQ_FMT("CRT_IV_SYNC", crt_iv_sync_in_fields,
			crt_iv_sync_out_fields);

static struct crt_req_format CQF_CRT_IV_FETCH =
	DEFINE_CRT_REQ_FMT("CRT_IV_FETCH", crt_iv_fetch_in_fields,
			crt_iv_fetch_out_fields);

static struct crt_req_format CQF_CRT_IV_UPDATE =
	DEFINE_CRT_REQ_FMT("CRT_IV_UPDATE", crt_iv_update_in_fields,
			crt_iv_update_out_fields);

/* barrier */
static struct crt_msg_field *crt_barrier_in_fields[] = {
	&CMF_INT,		/* enter_num */
};

static struct crt_msg_field *crt_barrier_out_fields[] = {
	&CMF_INT,		/* barrier_rc */
};

static struct crt_req_format CQF_CRT_BARRIER =
	DEFINE_CRT_REQ_FMT("CRT_BARRIER", crt_barrier_in_fields,
			   crt_barrier_out_fields);

static struct crt_corpc_ops crt_barrier_corpc_ops = {
	.co_aggregate = crt_hdlr_barrier_aggregate,
};

/* for broadcasting RAS notifications on rank failures */
struct crt_msg_field *crt_lm_evict_in_fields[] = {
	&CMF_RANK,		/* failed rank */
	&CMF_UINT32,		/* version number */
};

struct crt_msg_field *crt_lm_evict_out_fields[] = {
	&CMF_INT,		/* var for the aggregation function */
	&CMF_INT,		/* return value */
};

static struct crt_req_format CQF_CRT_LM_EVICT =
	DEFINE_CRT_REQ_FMT("CRT_LM_EVICT",
			   crt_lm_evict_in_fields,
			   crt_lm_evict_out_fields);

struct crt_msg_field *crt_lm_memb_sample_in_fields[] = {
	&CMF_UINT32,		/* client version NO. */
};

struct crt_msg_field *crt_lm_memb_sample_out_fields[] = {
	&CMF_IOVEC,	/* delta between the client's membership list and the
			 * service rank's membership list
			 */
	&CMF_UINT32,	/* server membership list version number */
	&CMF_INT,	/* return value */
};

static struct crt_req_format CQF_CRT_LM_MEMB_SAMPLE =
	DEFINE_CRT_REQ_FMT("CRT_LM_MEMB_SAMPLE",
			   crt_lm_memb_sample_in_fields,
			   crt_lm_memb_sample_out_fields);

struct crt_internal_rpc crt_internal_rpcs[] = {
	{
		.ir_name	= "CRT_GRP_CREATE",
		.ir_opc		= CRT_OPC_GRP_CREATE,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_GRP_CREATE,
		.ir_hdlr	= crt_hdlr_grp_create,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_GRP_DESTROY",
		.ir_opc		= CRT_OPC_GRP_DESTROY,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_GRP_DESTROY,
		.ir_hdlr	= crt_hdlr_grp_destroy,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_URI_LOOKUP",
		.ir_opc		= CRT_OPC_URI_LOOKUP,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_URI_LOOKUP,
		.ir_hdlr	= crt_hdlr_uri_lookup,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_BOTH_EMPTY",
		.ir_opc		= CRT_OPC_SELF_TEST_BOTH_EMPTY,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= NULL, /* No payload in either direction */
		.ir_hdlr	= crt_self_test_msg_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_SEND_EMPTY_REPLY_IOV",
		.ir_opc		= CRT_OPC_SELF_TEST_SEND_EMPTY_REPLY_IOV,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_SEND_EMPTY_REPLY_IOV,
		.ir_hdlr	= crt_self_test_msg_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_SEND_IOV_REPLY_EMPTY",
		.ir_opc		= CRT_OPC_SELF_TEST_SEND_IOV_REPLY_EMPTY,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_SEND_IOV_REPLY_EMPTY,
		.ir_hdlr	= crt_self_test_msg_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_BOTH_IOV",
		.ir_opc		= CRT_OPC_SELF_TEST_BOTH_IOV,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_BOTH_IOV,
		.ir_hdlr	= crt_self_test_msg_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_SEND_BULK_REPLY_IOV",
		.ir_opc		= CRT_OPC_SELF_TEST_SEND_BULK_REPLY_IOV,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_SEND_BULK_REPLY_IOV,
		.ir_hdlr	= crt_self_test_msg_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_SEND_IOV_REPLY_BULK",
		.ir_opc		= CRT_OPC_SELF_TEST_SEND_IOV_REPLY_BULK,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_SEND_IOV_REPLY_BULK,
		.ir_hdlr	= crt_self_test_msg_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_BOTH_BULK",
		.ir_opc		= CRT_OPC_SELF_TEST_BOTH_BULK,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_BOTH_BULK,
		.ir_hdlr	= crt_self_test_msg_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_OPEN_SESSION",
		.ir_opc		= CRT_OPC_SELF_TEST_OPEN_SESSION,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_OPEN_SESSION,
		.ir_hdlr	= crt_self_test_open_session_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_CLOSE_SESSION",
		.ir_opc		= CRT_OPC_SELF_TEST_CLOSE_SESSION,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_CLOSE_SESSION,
		.ir_hdlr	= crt_self_test_close_session_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_START",
		.ir_opc		= CRT_OPC_SELF_TEST_START,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_START,
		.ir_hdlr	= crt_self_test_start_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_SELF_TEST_STATUS_REQ",
		.ir_opc		= CRT_OPC_SELF_TEST_STATUS_REQ,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_SELF_TEST_STATUS_REQ,
		.ir_hdlr	= crt_self_test_status_req_handler,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_IV_FETCH",
		.ir_opc		= CRT_OPC_IV_FETCH,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_IV_FETCH,
		.ir_hdlr	= crt_hdlr_iv_fetch,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_IV_UPDATE",
		.ir_opc		= CRT_OPC_IV_UPDATE,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_IV_UPDATE,
		.ir_hdlr	= crt_hdlr_iv_update,
		.ir_co_ops	= NULL,
	}, {
		.ir_name	= "CRT_IV_SYNC",
		.ir_opc		= CRT_OPC_IV_SYNC,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_IV_SYNC,
		.ir_hdlr	= crt_hdlr_iv_sync,
		.ir_co_ops	= &crt_iv_sync_co_ops,
	}, {
		.ir_name	= "CRT_BARRIER_ENTER",
		.ir_opc		= CRT_OPC_BARRIER_ENTER,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_BARRIER,
		.ir_hdlr	= crt_hdlr_barrier_enter,
		.ir_co_ops	= &crt_barrier_corpc_ops,
	}, {
		.ir_name	= "CRT_BARRIER_EXIT",
		.ir_opc		= CRT_OPC_BARRIER_EXIT,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_BARRIER,
		.ir_hdlr	= crt_hdlr_barrier_exit,
		.ir_co_ops	= &crt_barrier_corpc_ops,
	}, {
		.ir_name	= "CRT_RANK_EVICT",
		.ir_opc		= CRT_OPC_RANK_EVICT,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_LM_EVICT,
		.ir_hdlr	= crt_hdlr_rank_evict,
		.ir_co_ops	= &crt_rank_evict_co_ops,
	}, {
		.ir_name	= "CRT_MEMB_SAMPLE",
		.ir_opc		= CRT_OPC_MEMB_SAMPLE,
		.ir_ver		= 1,
		.ir_flags	= 0,
		.ir_req_fmt	= &CQF_CRT_LM_MEMB_SAMPLE,
		.ir_hdlr	= crt_hdlr_memb_sample,
		.ir_co_ops	= NULL,
	}, {
		.ir_opc		= 0
	}
};

/* CRT RPC related APIs or internal functions */

int
crt_internal_rpc_register(void)
{
	struct crt_internal_rpc *rpc;
	int	rc = 0;

	/* walk through the handler list and register each individual RPC */
	for (rpc = crt_internal_rpcs; rpc->ir_opc != 0; rpc++) {
		D_ASSERT(rpc->ir_hdlr != NULL);
		rc = crt_rpc_reg_internal(rpc->ir_opc, rpc->ir_req_fmt,
					  rpc->ir_hdlr, rpc->ir_co_ops);
		if (rc) {
			D_ERROR("opcode 0x%x registration failed, rc: %d.\n",
				rpc->ir_opc, rc);
			break;
		}
	}
	return rc;
}

int
crt_rpc_priv_alloc(crt_opcode_t opc, struct crt_rpc_priv **priv_allocated,
		   bool forward)
{
	struct crt_rpc_priv	*rpc_priv;
	struct crt_opc_info	*opc_info;
	int			rc = 0;

	D_ASSERT(priv_allocated != NULL);

	opc_info = crt_opc_lookup(crt_gdata.cg_opc_map, opc, CRT_UNLOCK);
	if (opc_info == NULL) {
		D_ERROR("opc: 0x%x, lookup failed.\n", opc);
		D_GOTO(out, rc = -DER_UNREG);
	}
	D_ASSERT(opc_info->coi_input_size <= CRT_MAX_INPUT_SIZE &&
		 opc_info->coi_output_size <= CRT_MAX_OUTPUT_SIZE);

	if (forward)
		D_ALLOC(rpc_priv, opc_info->coi_input_offset);
	else
		D_ALLOC(rpc_priv, opc_info->coi_rpc_size);
	if (rpc_priv == NULL)
		D_GOTO(out, rc = -DER_NOMEM);

	rpc_priv->crp_opc_info = opc_info;
	rpc_priv->crp_forward = forward;
	*priv_allocated = rpc_priv;

	D_DEBUG("rpc_priv %p (opc: 0x%x), allocated.\n",
		rpc_priv, rpc_priv->crp_opc_info->coi_opc);

out:
	return rc;
}

void
crt_rpc_priv_free(struct crt_rpc_priv *rpc_priv)
{
	if (rpc_priv == NULL)
		return;

	if (rpc_priv->crp_coll && rpc_priv->crp_corpc_info) {
		d_rank_list_free(rpc_priv->crp_corpc_info->co_excluded_ranks);
		D_FREE_PTR(rpc_priv->crp_corpc_info);
	}

	if (rpc_priv->crp_uri_free != 0 && rpc_priv->crp_tgt_uri != NULL)
		D_FREE(rpc_priv->crp_tgt_uri, CRT_ADDR_STR_MAX_LEN);

	pthread_spin_destroy(&rpc_priv->crp_lock);

	if (rpc_priv->crp_forward)
		D_FREE(rpc_priv, rpc_priv->crp_opc_info->coi_input_offset);
	else
		D_FREE(rpc_priv, rpc_priv->crp_opc_info->coi_rpc_size);
}

int
crt_req_create_internal(crt_context_t crt_ctx, crt_endpoint_t *tgt_ep,
			crt_opcode_t opc, bool forward, crt_rpc_t **req)
{
	struct crt_rpc_priv	*rpc_priv = NULL;
	crt_rpc_t		*rpc_pub;
	int			 rc = 0;

	D_ASSERT(crt_ctx != CRT_CONTEXT_NULL && req != NULL);

	rc = crt_rpc_priv_alloc(opc, &rpc_priv, forward);
	if (rc != 0) {
		D_ERROR("crt_rpc_priv_alloc, rc: %d, opc: 0x%x.\n", rc, opc);
		D_GOTO(out, rc);
	}
	D_ASSERT(rpc_priv != NULL);
	rpc_pub = &rpc_priv->crp_pub;
	if (tgt_ep != NULL) {
		rpc_pub->cr_ep.ep_rank = tgt_ep->ep_rank;
		rpc_pub->cr_ep.ep_tag = tgt_ep->ep_tag;
		rpc_pub->cr_ep.ep_grp = tgt_ep->ep_grp;
		rpc_priv->crp_have_ep = 1;
	}

	crt_rpc_priv_init(rpc_priv, crt_ctx, opc, false /* srv_flag */);

	*req = rpc_pub;

out:
	if (rc < 0)
		crt_rpc_priv_free(rpc_priv);
	return rc;
}

static int check_ep(crt_endpoint_t *tgt_ep)
{
	struct crt_grp_priv	*grp_priv;
	int rc = 0;

	if (tgt_ep->ep_grp == NULL) {
		grp_priv = crt_gdata.cg_grp->gg_srv_pri_grp;
		if (grp_priv == NULL) {
			D_ERROR("service group not attached yet.\n");
			D_GOTO(out, rc = -DER_NOTATTACH);
		}
	} else {
		grp_priv = container_of(tgt_ep->ep_grp, struct crt_grp_priv,
					gp_pub);
		if (grp_priv->gp_primary == 0 || grp_priv->gp_service == 0) {
			D_ERROR("bad parameter tgt_ep->ep_grp: %p (gp_primary: "
				"%d, gp_service: %d, gp_local: %d.\n",
				tgt_ep->ep_grp, grp_priv->gp_primary,
				grp_priv->gp_service, grp_priv->gp_local);
			D_GOTO(out, rc = -DER_INVAL);
		}
	}
	if (tgt_ep->ep_rank >= grp_priv->gp_size) {
		D_ERROR("invalid parameter, rank %d, group_size: %d.\n",
			tgt_ep->ep_rank, grp_priv->gp_size);
		D_GOTO(out, rc = -DER_INVAL);
	}

out:
	return rc;
}

int
crt_req_create(crt_context_t crt_ctx, crt_endpoint_t *tgt_ep, crt_opcode_t opc,
	       crt_rpc_t **req)
{
	int rc = 0;

	if (crt_ctx == CRT_CONTEXT_NULL || req == NULL) {
		D_ERROR("invalid parameter (NULL crt_ctx or req).\n");
		D_GOTO(out, rc = -DER_INVAL);
	}
	if (!crt_initialized()) {
		D_ERROR("CRT not initialized.\n");
		D_GOTO(out, rc = -DER_UNINIT);
	}
	if (tgt_ep != NULL) {
		rc = check_ep(tgt_ep);
		if (rc != 0)
			D_GOTO(out, rc);
	}

	rc = crt_req_create_internal(crt_ctx, tgt_ep, opc, false /* forward */,
				     req);
	if (rc != 0) {
		D_ERROR("crt_req_create_internal failed, opc: 0x%x, rc: %d.\n",
			opc, rc);
		D_GOTO(out, rc);
	}
	D_ASSERT(*req != NULL);

out:
	return rc;
}

int
crt_req_set_endpoint(crt_rpc_t *req, crt_endpoint_t *tgt_ep)
{
	struct crt_rpc_priv	*rpc_priv;
	int			 rc = 0;

	if (req == NULL || tgt_ep == NULL) {
		D_ERROR("invalid parameter (NULL req or tgt_ep).\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	rpc_priv = container_of(req, struct crt_rpc_priv, crp_pub);
	if (rpc_priv->crp_have_ep == 1) {
		D_ERROR("target endpoint already set.\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	rc = check_ep(tgt_ep);
	if (rc != 0)
		D_GOTO(out, rc);

	req->cr_ep.ep_rank = tgt_ep->ep_rank;
	req->cr_ep.ep_tag = tgt_ep->ep_tag;
	req->cr_ep.ep_grp = tgt_ep->ep_grp;
	rpc_priv->crp_have_ep = 1;

	D_DEBUG("rpc_priv %p ep modified %u.%u.\n",
		rpc_priv, req->cr_ep.ep_rank, req->cr_ep.ep_tag);

out:
	return rc;
}

int
crt_req_set_timeout(crt_rpc_t *req, uint32_t timeout_sec)
{
	struct crt_rpc_priv	*rpc_priv;
	int			 rc = 0;

	if (req == NULL || timeout_sec == 0) {
		D_ERROR("invalid parameter (NULL req or zero timeout_sec).\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	rpc_priv = container_of(req, struct crt_rpc_priv, crp_pub);
	rpc_priv->crp_timeout_sec = timeout_sec;

out:
	return rc;
}

int
crt_req_addref(crt_rpc_t *req)
{
	struct crt_rpc_priv	*rpc_priv;
	int			rc = 0;

	if (req == NULL) {
		D_ERROR("invalid parameter (NULL req).\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	rpc_priv = container_of(req, struct crt_rpc_priv, crp_pub);
	pthread_spin_lock(&rpc_priv->crp_lock);
	rpc_priv->crp_refcount++;
	D_DEBUG("rpc_priv %p (opc: 0x%x), addref to %d.\n",
		rpc_priv, req->cr_opc, rpc_priv->crp_refcount);
	pthread_spin_unlock(&rpc_priv->crp_lock);

out:
	return rc;
}

int
crt_req_decref(crt_rpc_t *req)
{
	struct crt_rpc_priv	*rpc_priv;
	int			rc = 0, destroy = 0;

	if (req == NULL) {
		D_ERROR("invalid parameter (NULL req).\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	rpc_priv = container_of(req, struct crt_rpc_priv, crp_pub);
	pthread_spin_lock(&rpc_priv->crp_lock);
	rpc_priv->crp_refcount--;
	if (rpc_priv->crp_refcount == 0)
		destroy = 1;
	D_DEBUG("rpc_priv %p (opc: 0x%x), decref to %d.\n",
		rpc_priv, req->cr_opc, rpc_priv->crp_refcount);
	pthread_spin_unlock(&rpc_priv->crp_lock);

	if (destroy == 1) {
		if (rpc_priv->crp_reply_pending == 1) {
			D_WARN("no reply sent for rpc_priv %p (opc: 0x%x).\n",
			       rpc_priv, req->cr_opc);
			/* We have executed the user RPC handler, but the user
			 * handler forgot to call crt_reply_send(). We send a
			 * CART level error message to notify the client
			 */
			crt_hg_reply_error_send(rpc_priv, -DER_NOREPLY);
		}

		rc = crt_hg_req_destroy(rpc_priv);
		if (rc != 0)
			D_ERROR("crt_hg_req_destroy failed, rc: %d, "
				"rpc_priv %p(opc: 0x%x).\n",
				rc, rpc_priv, req->cr_opc);
	}

out:
	return rc;
}

static int
crt_req_hg_addr_lookup_cb(hg_addr_t hg_addr, void *priv)
{
	struct crt_rpc_priv		*rpc_priv;
	d_rank_t			 rank;
	struct crt_grp_priv		*grp_priv;
	struct crt_context		*crt_ctx;
	int				 ctx_idx;
	int				 tag;
	int				 rc = 0;

	rpc_priv = (struct crt_rpc_priv *)priv;
	D_ASSERT(rpc_priv != NULL);
	rank = rpc_priv->crp_pub.cr_ep.ep_rank;
	tag = rpc_priv->crp_pub.cr_ep.ep_tag;

	if (rpc_priv->crp_pub.cr_ep.ep_grp == NULL)
		grp_priv = crt_gdata.cg_grp->gg_srv_pri_grp;
	else
		grp_priv = container_of(rpc_priv->crp_pub.cr_ep.ep_grp,
					struct crt_grp_priv, gp_pub);
	crt_ctx = (struct crt_context *)rpc_priv->crp_pub.cr_ctx;
	ctx_idx = crt_ctx->cc_idx;

	rc = crt_grp_lc_addr_insert(grp_priv, crt_ctx, rank, tag, &hg_addr);
	if (rc != 0) {
		D_ERROR("crt_grp_lc_addr_insert() failed. rc %d "
			"grp_priv %p ctx_idx %d, rank: %d, tag %d.\n",
			rc, grp_priv, ctx_idx, rank, tag);
		D_GOTO(out, rc);
	}
	rpc_priv->crp_hg_addr = hg_addr;
	rc = crt_req_send_internal(rpc_priv);
	if (rc != 0) {
		D_ERROR("crt_req_send_internal() failed, rc %d, rpc_priv: %p, "
			"opc: 0x%x.\n", rc, rpc_priv, rpc_priv->crp_pub.cr_opc);
		D_GOTO(out, rc);
	}
out:
	if (rc != 0) {
		crt_context_req_untrack(&rpc_priv->crp_pub);
		crt_rpc_complete(rpc_priv, rc);
		crt_req_decref(&rpc_priv->crp_pub); /* destroy */
	}
	/* addref in crt_req_hg_addr_lookup */
	crt_req_decref(&rpc_priv->crp_pub);
	return rc;
}

static inline int
crt_req_get_tgt_uri(struct crt_rpc_priv *rpc_priv, crt_phy_addr_t base_uri)
{
	int		rc;

	D_ASSERT(rpc_priv != NULL);
	D_ASSERT(base_uri != NULL);

	rpc_priv->crp_tgt_uri  =
		crt_get_tag_uri(base_uri, rpc_priv->crp_pub.cr_ep.ep_tag);
	if (rpc_priv->crp_tgt_uri == NULL) {
		D_ERROR("crt_get_tag_uri failed, opc: 0x%x.\n",
			rpc_priv->crp_pub.cr_opc);
		rc = -DER_NOMEM;
	} else {
		rpc_priv->crp_uri_free = 1;
		rc = 0;
	}

	return rc;
}

static void
crt_req_uri_lookup_psr_cb(const struct crt_cb_info *cb_info)
{
	d_rank_t			 rank;
	crt_endpoint_t			*tgt_ep;
	struct crt_rpc_priv		*rpc_priv;
	struct crt_grp_priv		*grp_priv;
	struct crt_context		*crt_ctx;
	struct crt_uri_lookup_out	*ul_out;
	char				*uri = NULL;
	int				 rc = 0;

	rpc_priv = (struct crt_rpc_priv *)cb_info->cci_arg;
	D_ASSERT(rpc_priv->crp_state == RPC_STATE_URI_LOOKUP);
	D_ASSERT(rpc_priv->crp_ul_req = cb_info->cci_rpc);

	if (cb_info->cci_rc != 0) {
		D_ERROR("rpc_priv %p(opc: 0x%x), failed cci_rc: %d.\n",
			container_of(cb_info->cci_rpc, struct crt_rpc_priv,
				     crp_pub),
			cb_info->cci_rpc->cr_opc, cb_info->cci_rc);
		D_GOTO(out, rc = cb_info->cci_rc);
	}

	tgt_ep = &rpc_priv->crp_pub.cr_ep;
	rank = tgt_ep->ep_rank;
	if (tgt_ep->ep_grp == NULL)
		grp_priv = crt_gdata.cg_grp->gg_srv_pri_grp;
	else
		grp_priv = container_of(tgt_ep->ep_grp, struct crt_grp_priv,
					gp_pub);
	crt_ctx = (struct crt_context *)rpc_priv->crp_pub.cr_ctx;
	/* extract uri */
	ul_out = crt_reply_get(cb_info->cci_rpc);
	D_ASSERT(ul_out != NULL);
	uri = ul_out->ul_uri;

	/* insert uri to hash table */
	rc = crt_grp_lc_uri_insert(grp_priv, crt_ctx->cc_idx, rank, uri);
	if (rc != 0) {
		D_ERROR("crt_grp_lc_uri_insert() failed, rc %d\n", rc);
		D_GOTO(out, rc);
	}

	rc = crt_req_get_tgt_uri(rpc_priv, uri);
	if (rc != 0) {
		D_ERROR("crt_req_get_tgt_uri failed, opc: 0x%x.\n",
			rpc_priv->crp_pub.cr_opc);
		D_GOTO(out, rc);
	}

	rc = crt_req_send_internal(rpc_priv);
	if (rc != 0) {
		D_ERROR("crt_req_send_internal() failed, rc %d, opc: 0x%x\n",
			rc, rpc_priv->crp_pub.cr_opc);
		D_GOTO(out, rc);
	}
out:
	if (rc != 0) {
		crt_context_req_untrack(&rpc_priv->crp_pub);
		crt_rpc_complete(rpc_priv, rc);
		crt_req_decref(&rpc_priv->crp_pub); /* destroy */
	}

	/* addref in crt_req_uri_lookup_psr */
	crt_req_decref(rpc_priv->crp_ul_req);
	rpc_priv->crp_ul_req = NULL;
	/* addref in crt_req_uri_lookup_psr */
	crt_req_decref(&rpc_priv->crp_pub);
}

/*
 * Contact the PSR to obtain the URI of the target rank which rpc_priv is trying
 * to communicate to
 */
int
crt_req_uri_lookup_psr(struct crt_rpc_priv *rpc_priv, crt_cb_t complete_cb,
		       void *arg)
{
	crt_rpc_t			*ul_req;
	crt_endpoint_t			*tgt_ep;
	crt_endpoint_t			 psr_ep = {0};
	struct crt_grp_priv		*grp_priv;
	struct crt_uri_lookup_in	*ul_in;
	struct crt_uri_lookup_out	*ul_out;
	int				 rc = 0;

	tgt_ep = &rpc_priv->crp_pub.cr_ep;
	if (tgt_ep->ep_grp == NULL)
		grp_priv = crt_gdata.cg_grp->gg_srv_pri_grp;
	else
		grp_priv = container_of(tgt_ep->ep_grp, struct crt_grp_priv,
					gp_pub);

	psr_ep.ep_grp = tgt_ep->ep_grp;
	psr_ep.ep_rank = grp_priv->gp_psr_rank;

	rc = crt_req_create(rpc_priv->crp_pub.cr_ctx, &psr_ep,
			    CRT_OPC_URI_LOOKUP, &ul_req);
	if (rc != 0) {
		D_ERROR("crt_req_create URI_LOOKUP failed, rc: %d opc: 0x%x.\n",
			rc, rpc_priv->crp_pub.cr_opc);
		D_GOTO(out, rc);
	}
	/* decref in crt_req_uri_lookup_psr_cb */
	crt_req_addref(ul_req);
	/* decref in crt_req_uri_lookup_psr_cb */
	crt_req_addref(&rpc_priv->crp_pub);
	rpc_priv->crp_ul_req = ul_req;
	ul_in = crt_req_get(ul_req);
	ul_out = crt_reply_get(ul_req);
	D_ASSERT(ul_in != NULL && ul_out != NULL);
	ul_in->ul_grp_id = grp_priv->gp_pub.cg_grpid;
	ul_in->ul_rank = rpc_priv->crp_pub.cr_ep.ep_rank;
	rc = crt_req_send(ul_req, complete_cb, arg);
	if (rc != 0) {
		D_ERROR("URI_LOOKUP (to group %s rank %d through PSR %d) "
			"request send failed, rc: %d opc: 0x%x.\n",
			ul_in->ul_grp_id, ul_in->ul_rank, psr_ep.ep_rank,
			rc, rpc_priv->crp_pub.cr_opc);
		crt_req_decref(ul_req); /* rollback addref above */
		crt_req_decref(&rpc_priv->crp_pub); /* rollback addref above */
	}

out:
	return rc;
}

/* look in the local cache to find the NA address of the target */
static int
crt_req_ep_lc_lookup(struct crt_rpc_priv *rpc_priv, crt_phy_addr_t *base_addr)
{
	struct crt_grp_priv	*grp_priv;
	crt_rpc_t		*req;
	crt_endpoint_t		*tgt_ep;
	struct crt_context	*ctx;
	int			 rc = 0;

	req = &rpc_priv->crp_pub;
	ctx = req->cr_ctx;
	tgt_ep = &req->cr_ep;

	if (tgt_ep->ep_grp == NULL)
		grp_priv = crt_gdata.cg_grp->gg_srv_pri_grp;
	else
		grp_priv = container_of(tgt_ep->ep_grp, struct crt_grp_priv,
					gp_pub);
	rc = crt_grp_lc_lookup(grp_priv, ctx->cc_idx,
			       tgt_ep->ep_rank, tgt_ep->ep_tag, base_addr,
			       &rpc_priv->crp_hg_addr);
	if (rc != 0) {
		D_ERROR("crt_grp_lc_lookup failed, rc: %d, opc: 0x%x.\n",
			rc, rpc_priv->crp_pub.cr_opc);
		D_GOTO(out, rc);
	}

	/*
	 * If the target endpoint is the PSR and it's not already in the address
	 * cache, insert the URI of the PSR to the address cache.
	 * Did it in crt_grp_attach(), in the case that this context created
	 * later can insert it here.
	 */
	if (*base_addr == NULL && grp_priv->gp_local == 0
	    && tgt_ep->ep_rank == grp_priv->gp_psr_rank) {
		*base_addr = grp_priv->gp_psr_phy_addr;
		rc = crt_grp_lc_uri_insert(grp_priv, ctx->cc_idx,
					   tgt_ep->ep_rank,
					   grp_priv->gp_psr_phy_addr);
		if (rc != 0)
			D_ERROR("crt_grp_lc_uri_insert() failed. rc: %d\n", rc);
	}

out:
	return rc;
}

static bool
crt_req_is_self(struct crt_rpc_priv *rpc_priv)
{
	struct crt_grp_priv	*grp_priv_self;
	crt_endpoint_t		*tgt_ep;
	bool			 same_group;
	bool			 same_rank;

	D_ASSERT(rpc_priv != NULL);
	grp_priv_self = crt_grp_pub2priv(NULL);
	tgt_ep = &rpc_priv->crp_pub.cr_ep;
	same_group = (tgt_ep->ep_grp == NULL) ||
		     crt_grp_id_identical(tgt_ep->ep_grp->cg_grpid,
					  grp_priv_self->gp_pub.cg_grpid);
	same_rank = tgt_ep->ep_rank == grp_priv_self->gp_self;

	return (same_group && same_rank);
}

/*
 * the case where we don't have the URI of the target rank
 */
static int
crt_req_uri_lookup(struct crt_rpc_priv *rpc_priv)
{
	d_rank_t		 rank;
	crt_endpoint_t		*tgt_ep;
	struct crt_grp_priv	*grp_priv;
	crt_group_id_t		 grp_id;
	char			*uri = NULL;
	struct crt_context	*crt_ctx;
	int			 rc = 0;

	tgt_ep = &rpc_priv->crp_pub.cr_ep;
	if (tgt_ep->ep_grp == NULL)
		grp_priv = crt_gdata.cg_grp->gg_srv_pri_grp;
	else
		grp_priv = container_of(tgt_ep->ep_grp, struct crt_grp_priv,
					gp_pub);
	D_ASSERT(grp_priv != NULL);

	/* this is a remote group, contact the PSR */
	if (grp_priv->gp_local == 0) {
		/* send an RPC to the PSR */
		D_DEBUG("Querying PSR to find out target NA Address.\n");
		rc = crt_req_uri_lookup_psr(rpc_priv, crt_req_uri_lookup_psr_cb,
					rpc_priv);
		if (rc != 0) {
			rpc_priv->crp_state = RPC_STATE_INITED;
			D_ERROR("crt_grp_uri_lookup_psr() failed, rc %d.\n",
				rc);
		}
		D_GOTO(out, rc);
	}

	rank = tgt_ep->ep_rank;
	crt_ctx = (struct crt_context *)rpc_priv->crp_pub.cr_ctx;
	if (crt_req_is_self(rpc_priv)) {
		/* rpc is sent to self */
		uri = strndup(crt_gdata.cg_addr, CRT_ADDR_STR_MAX_LEN);
		if (uri == NULL) {
			D_ERROR("strndup failed.\n");
			D_GOTO(out, rc = -DER_NOMEM);
		}
	} else {
		/* this is a local group, lookup through PMIx */
		grp_id = grp_priv->gp_pub.cg_grpid;
		rc = crt_pmix_uri_lookup(grp_id, rank, &uri);
		if (rc != 0) {
			D_ERROR("crt_pmix_uri_lookup() failed, rc %d.\n", rc);
			D_GOTO(out, rc);
		}
	}
	rc = crt_grp_lc_uri_insert(grp_priv, crt_ctx->cc_idx, rank, uri);
	if (rc != 0) {
		D_ERROR("crt_grp_lc_uri_insert() failed, rc %d\n", rc);
		D_GOTO(out, rc);
	}

	rc = crt_req_get_tgt_uri(rpc_priv, uri);
	if (rc != 0) {
		D_ERROR("crt_req_get_tgt_uri failed, opc: 0x%x.\n",
			rpc_priv->crp_pub.cr_opc);
		D_GOTO(out, rc);
	}
	rc = crt_req_send_internal(rpc_priv);
	if (rc != 0) {
		D_ERROR("crt_req_send_internal() failed, rc %d, opc: 0x%x\n",
			rc, rpc_priv->crp_pub.cr_opc);
		D_GOTO(out, rc);
	}
out:
	if (uri != NULL)
		free(uri);

	return rc;
}

/*
 * the case where we have the base URI but don't have the NA address of the tag
 */
static int
crt_req_hg_addr_lookup(struct crt_rpc_priv *rpc_priv)
{
	struct crt_context	*crt_ctx;
	int			 rc = 0;

	crt_ctx = (struct crt_context *)rpc_priv->crp_pub.cr_ctx;
	/* decref at crt_req_hg_addr_lookup_cb */
	crt_req_addref(&rpc_priv->crp_pub);
	rc = crt_hg_addr_lookup(&crt_ctx->cc_hg_ctx, rpc_priv->crp_tgt_uri,
				crt_req_hg_addr_lookup_cb, rpc_priv);
	if (rc != 0) {
		D_ERROR("crt_addr_lookup() failed, rc %d, opc: 0x%x..\n",
			rc, rpc_priv->crp_pub.cr_opc);
		/* rollback above addref */
		crt_req_decref(&rpc_priv->crp_pub);
	}
	return rc;
}

static inline int
crt_req_send_immediately(struct crt_rpc_priv *rpc_priv)
{
	crt_rpc_t			*req;
	struct crt_context		*ctx;
	int				 rc = 0;

	D_ASSERT(rpc_priv != NULL);
	D_ASSERT(rpc_priv->crp_hg_addr != NULL);

	req = &rpc_priv->crp_pub;
	ctx = (struct crt_context *)req->cr_ctx;
	rc = crt_hg_req_create(&ctx->cc_hg_ctx, rpc_priv);
	if (rc != 0) {
		D_ERROR("crt_hg_req_create failed, rc: %d, opc: 0x%x.\n",
			rc, req->cr_opc);
		D_GOTO(out, rc);
	}
	D_ASSERT(rpc_priv->crp_hg_hdl != NULL);

	/* set state ahead to avoid race with completion cb */
	rpc_priv->crp_state = RPC_STATE_REQ_SENT;
	rc = crt_hg_req_send(rpc_priv);
	if (rc != 0)
		D_ERROR("crt_hg_req_send failed, rc: %d, rpc_priv: %p,"
			"opc: 0x%x.\n", rc, rpc_priv, req->cr_opc);

out:
	if (rc != 0)
		D_ERROR("crt_req_send_immediately failed, rc: %d, rpc_priv: %p,"
			" opc: 0x%x.\n", rc, rpc_priv, req->cr_opc);
	return rc;
}

int
crt_req_send_internal(struct crt_rpc_priv *rpc_priv)
{
	crt_rpc_t			*req;
	crt_phy_addr_t			 base_addr = NULL;
	int				 rc = 0;

	req = &rpc_priv->crp_pub;
	switch (rpc_priv->crp_state) {
	case RPC_STATE_QUEUED:
		rpc_priv->crp_state = RPC_STATE_INITED;
	case RPC_STATE_INITED:
		/* lookup local cache  */
		rpc_priv->crp_hg_addr = NULL;
		rc = crt_req_ep_lc_lookup(rpc_priv, &base_addr);
		if (rc != 0) {
			D_ERROR("crt_grp_ep_lc_lookup() failed, rc %d, "
				"opc: 0x%x.\n", rc, req->cr_opc);
			D_GOTO(out, rc);
		}
		if (rpc_priv->crp_hg_addr != NULL) {
			/* send the RPC if the local cache has the HG_Addr */
			rc = crt_req_send_immediately(rpc_priv);
		} else if (base_addr != NULL) {
			/* send addr lookup req */
			rc = crt_req_get_tgt_uri(rpc_priv, base_addr);
			if (rc != 0) {
				D_ERROR("crt_req_get_tgt_uri failed, "
					"opc: 0x%x.\n", req->cr_opc);
				D_GOTO(out, rc);
			}
			rpc_priv->crp_state = RPC_STATE_ADDR_LOOKUP;
			rc = crt_req_hg_addr_lookup(rpc_priv);
			if (rc != 0)
				D_ERROR("crt_req_hg_addr_lookup() failed, "
					"rc %d, opc: 0x%x.\n", rc, req->cr_opc);
		} else {
			/* base_addr == NULL, send uri lookup req */
			rpc_priv->crp_state = RPC_STATE_URI_LOOKUP;
			rc = crt_req_uri_lookup(rpc_priv);
			if (rc != 0)
				D_ERROR("crt_req_uri_lookup() failed. rc %d, "
					"opc: 0x%x.\n", rc, req->cr_opc);
		}
		break;
	case RPC_STATE_URI_LOOKUP:
		rc = crt_req_ep_lc_lookup(rpc_priv, &base_addr);
		if (rc != 0) {
			D_ERROR("crt_grp_ep_lc_lookup() failed, rc %d, "
				"opc: 0x%x\n", rc, req->cr_opc);
			D_GOTO(out, rc);
		}
		if (rpc_priv->crp_hg_addr != NULL) {
			rc = crt_req_send_immediately(rpc_priv);
		} else {
			/* send addr lookup req */
			rpc_priv->crp_state = RPC_STATE_ADDR_LOOKUP;
			rc = crt_req_hg_addr_lookup(rpc_priv);
			if (rc != 0)
				D_ERROR("crt_req_hg_addr_lookup() failed, "
					"rc %d, opc: 0x%x.\n", rc, req->cr_opc);
		}
		break;
	case RPC_STATE_ADDR_LOOKUP:
		rc = crt_req_send_immediately(rpc_priv);
		break;
	default:
		D_ERROR("bad rpc state: 0x%x, opc: 0x%x.\n",
			rpc_priv->crp_state, req->cr_opc);
		rc = -DER_PROTO;
		break;
	}

out:
	if (rc != 0)
		rpc_priv->crp_state = RPC_STATE_INITED;
	return rc;
}

int
crt_req_send(crt_rpc_t *req, crt_cb_t complete_cb, void *arg)
{
	struct crt_rpc_priv	*rpc_priv = NULL;
	int			 rc = 0;

	if (req == NULL) {
		D_ERROR("invalid parameter (NULL req).\n");
		if (complete_cb != NULL) {
			struct crt_cb_info	cbinfo;

			cbinfo.cci_rpc = NULL;
			cbinfo.cci_arg = arg;
			cbinfo.cci_rc  = -DER_INVAL;
			complete_cb(&cbinfo);

			return 0;
		} else {
			return -DER_INVAL;
		}
	}

	if (req->cr_ctx == NULL) {
		D_ERROR("invalid parameter (NULL req->cr_ctx).\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	rpc_priv = container_of(req, struct crt_rpc_priv, crp_pub);
	rpc_priv->crp_complete_cb = complete_cb;
	rpc_priv->crp_arg = arg;

	if (rpc_priv->crp_coll) {
		rc = crt_corpc_req_hdlr(req);
		if (rc != 0)
			D_ERROR("crt_corpc_req_hdlr failed, "
				"rc: %d,opc: 0x%x.\n", rc, req->cr_opc);
		D_GOTO(out, rc);
	} else {
		if (!rpc_priv->crp_have_ep) {
			D_WARN("target endpoint not set "
				"rpc: %p, opc: 0x%x.\n", rpc_priv, req->cr_opc);
			D_GOTO(out, rc = -DER_INVAL);
		}
	}

	D_DEBUG("rpc_priv %p submitted.\n", rpc_priv);

	rc = crt_context_req_track(req);
	if (rc == CRT_REQ_TRACK_IN_INFLIGHQ) {
		/* tracked in crt_ep_inflight::epi_req_q */
		rc = crt_req_send_internal(rpc_priv);
		if (rc != 0) {
			D_ERROR("crt_req_send_internal() failed, "
				"rc %d, opc: 0x%x\n",
				rc, rpc_priv->crp_pub.cr_opc);
			crt_context_req_untrack(req);
		}
	} else if (rc == CRT_REQ_TRACK_IN_WAITQ) {
		/* queued in crt_hg_context::dhc_req_q */
		rc = 0;
	} else {
		D_ERROR("crt_req_track failed, rc: %d, opc: 0x%x.\n",
			rc, rpc_priv->crp_pub.cr_opc);
	}

out:
	/* internally destroy the req when failed */
	if (rc != 0) {
		if (!rpc_priv->crp_coll) {
			crt_rpc_complete(rpc_priv, rc);
			/* failure already reported through complete cb */
			if (complete_cb != NULL)
				rc = 0;
		}
		crt_req_decref(req);
	}
	return rc;
}

int
crt_reply_send(crt_rpc_t *req)
{
	struct crt_rpc_priv	*rpc_priv = NULL;
	int			rc = 0;

	if (req == NULL) {
		D_ERROR("invalid parameter (NULL req).\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	rpc_priv = container_of(req, struct crt_rpc_priv, crp_pub);

	if (rpc_priv->crp_coll == 1) {
		struct crt_cb_info	cb_info;

		cb_info.cci_rpc = &rpc_priv->crp_pub;
		cb_info.cci_rc = 0;
		cb_info.cci_arg = rpc_priv;

		crt_corpc_reply_hdlr(&cb_info);
	} else {
		rc = crt_hg_reply_send(rpc_priv);
		if (rc != 0)
			D_ERROR("crt_hg_reply_send failed, rc: %d,opc: 0x%x.\n",
				rc, rpc_priv->crp_pub.cr_opc);
	}

	rpc_priv->crp_reply_pending = 0;
out:
	return rc;
}

int
crt_req_abort(crt_rpc_t *req)
{
	struct crt_rpc_priv	*rpc_priv = NULL;
	int			rc = 0;

	if (req == NULL) {
		D_ERROR("invalid parameter (NULL req).\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	rpc_priv = container_of(req, struct crt_rpc_priv, crp_pub);

	if (crt_req_aborted(req)) {
		D_DEBUG("req (rpc_priv %p, opc: 0x%x) aborted, need not "
			"abort again.\n", rpc_priv, req->cr_opc);
		D_GOTO(out, rc);
	}

	rc = crt_hg_req_cancel(rpc_priv);
	if (rc != 0) {
		D_ERROR("crt_hg_req_cancel failed, rc: %d, opc: 0x%x.\n",
			rc, rpc_priv->crp_pub.cr_opc);
	}

out:
	return rc;
}

static void
crt_cb_common(const struct crt_cb_info *cb_info)
{
	*(int *)cb_info->cci_arg = 1;
}

/**
 * Send rpc synchronously
 *
 * \param[IN] rpc	point to CRT request.
 * \param[IN] timeout	timeout (Micro-seconds) to wait, if
 *                      timeout <= 0, it will wait infinitely.
 * \return		0 if rpc return successfully.
 * \return		negative errno if sending fails or timeout.
 */
int
crt_req_send_sync(crt_rpc_t *rpc, uint64_t timeout)
{
	uint64_t now;
	uint64_t end;
	int rc;
	int complete = 0;

	/* Send request */
	rc = crt_req_send(rpc, crt_cb_common, &complete);
	if (rc != 0)
		return rc;

	/* Check if we are lucky */
	if (complete)
		return 0;

	timeout = timeout ? timeout : CRT_DEFAULT_TIMEOUT_US;
	/* Wait the request to be completed in timeout milliseconds */
	end = d_timeus_secdiff(0) + timeout;

	while (1) {
		uint64_t interval = 1000; /* microseconds */

		rc = crt_progress(rpc->cr_ctx, interval, NULL, NULL);
		if (rc != 0 && rc != -DER_TIMEDOUT) {
			D_ERROR("crt_progress failed rc: %d.\n", rc);
			break;
		}

		if (complete) {
			rc = 0;
			break;
		}

		now = d_timeus_secdiff(0);
		if (now >= end) {
			rc = -DER_TIMEDOUT;
			break;
		}
	}

	return rc;
}

static void
crt_rpc_inout_buff_fini(struct crt_rpc_priv *rpc_priv)
{
	crt_rpc_t	*rpc_pub;

	D_ASSERT(rpc_priv != NULL);
	rpc_pub = &rpc_priv->crp_pub;

	if (rpc_pub->cr_input != NULL) {
		D_ASSERT(rpc_pub->cr_input_size != 0);
		rpc_pub->cr_input_size = 0;
		rpc_pub->cr_input = NULL;
	}

	if (rpc_pub->cr_output != NULL) {
		rpc_pub->cr_output_size = 0;
		rpc_pub->cr_output = NULL;
	}
}

static void
crt_rpc_inout_buff_init(struct crt_rpc_priv *rpc_priv)
{
	crt_rpc_t		*rpc_pub;
	struct crt_opc_info	*opc_info;

	D_ASSERT(rpc_priv != NULL);
	rpc_pub = &rpc_priv->crp_pub;
	D_ASSERT(rpc_pub->cr_input == NULL);
	D_ASSERT(rpc_pub->cr_output == NULL);
	opc_info = rpc_priv->crp_opc_info;
	D_ASSERT(opc_info != NULL);

	/*
	 * for forward request, need not allocate memory here, instead it will
	 * reuse the original input buffer of parent RPC.
	 * See crt_corpc_req_hdlr().
	 */
	if (opc_info->coi_input_size > 0 && !rpc_priv->crp_forward) {
		rpc_pub->cr_input = ((void *)rpc_priv) +
			opc_info->coi_input_offset;
		rpc_pub->cr_input_size = opc_info->coi_input_size;
	}
	if (opc_info->coi_output_size > 0) {
		rpc_pub->cr_output = ((void *)rpc_priv) +
			opc_info->coi_output_offset;
		rpc_pub->cr_output_size = opc_info->coi_output_size;
	}
}

void
crt_rpc_priv_init(struct crt_rpc_priv *rpc_priv, crt_context_t crt_ctx,
		  crt_opcode_t opc, bool srv_flag)
{
	D_ASSERT(rpc_priv != NULL);
	D_INIT_LIST_HEAD(&rpc_priv->crp_epi_link);
	D_INIT_LIST_HEAD(&rpc_priv->crp_tmp_link);
	D_INIT_LIST_HEAD(&rpc_priv->crp_parent_link);
	rpc_priv->crp_complete_cb = NULL;
	rpc_priv->crp_arg = NULL;
	if (!srv_flag) {
		crt_common_hdr_init(&rpc_priv->crp_req_hdr, opc);
		crt_common_hdr_init(&rpc_priv->crp_reply_hdr, opc);
	}
	rpc_priv->crp_state = RPC_STATE_INITED;
	rpc_priv->crp_hdl_reuse = NULL;
	rpc_priv->crp_srv = srv_flag;
	/* initialize as 1, so user can cal crt_req_decref to destroy new req */
	rpc_priv->crp_refcount = 1;
	pthread_spin_init(&rpc_priv->crp_lock, PTHREAD_PROCESS_PRIVATE);

	rpc_priv->crp_pub.cr_opc = opc;
	rpc_priv->crp_pub.cr_ctx = crt_ctx;

	crt_rpc_inout_buff_init(rpc_priv);
}

void
crt_rpc_priv_fini(struct crt_rpc_priv *rpc_priv)
{
	D_ASSERT(rpc_priv != NULL);
	crt_rpc_inout_buff_fini(rpc_priv);
}

static void
crt_handle_rpc(void *arg)
{
	struct crt_rpc_priv	*rpc_priv = arg;
	crt_rpc_t		*rpc_pub;

	D_ASSERT(rpc_priv != NULL);
	D_ASSERT(rpc_priv->crp_opc_info != NULL);
	D_ASSERT(rpc_priv->crp_opc_info->coi_rpc_cb != NULL);
	rpc_pub = &rpc_priv->crp_pub;
	rpc_priv->crp_opc_info->coi_rpc_cb(rpc_pub);
	crt_req_decref(rpc_pub);
}

int
crt_rpc_common_hdlr(struct crt_rpc_priv *rpc_priv)
{
	struct crt_context	*crt_ctx;
	int			 rc = 0;

	D_ASSERT(rpc_priv != NULL);
	crt_ctx = (struct crt_context *)rpc_priv->crp_pub.cr_ctx;

	/* Set the reply pending bit unless this is a one-way OPCODE */
	if (!rpc_priv->crp_opc_info->coi_no_reply)
		rpc_priv->crp_reply_pending = 1;

	if (crt_ctx->cc_pool != NULL) {
		rc = ABT_thread_create(*(ABT_pool *)crt_ctx->cc_pool,
				       crt_handle_rpc, rpc_priv,
				       ABT_THREAD_ATTR_NULL, NULL);
	} else {
		rpc_priv->crp_opc_info->coi_rpc_cb(&rpc_priv->crp_pub);
	}

	return rc;
}

static int
timeout_bp_node_enter(struct d_binheap *h, struct d_binheap_node *e)
{
	struct crt_rpc_priv	*rpc_priv;

	D_ASSERT(h != NULL);
	D_ASSERT(e != NULL);

	rpc_priv = container_of(e, struct crt_rpc_priv, crp_timeout_bp_node);

	D_DEBUG("rpc_priv %p (opc 0x%x) entering the timeout binheap.\n",
		rpc_priv, rpc_priv->crp_pub.cr_opc);

	return 0;
}

static int
timeout_bp_node_exit(struct d_binheap *h, struct d_binheap_node *e)
{
	struct crt_rpc_priv	*rpc_priv;

	D_ASSERT(h != NULL);
	D_ASSERT(e != NULL);

	rpc_priv = container_of(e, struct crt_rpc_priv, crp_timeout_bp_node);

	D_DEBUG("rpc_priv %p (opc 0x%x) exiting the timeout binheap.\n",
		rpc_priv, rpc_priv->crp_pub.cr_opc);

	return 0;
}

static bool
timeout_bp_node_cmp(struct d_binheap_node *a, struct d_binheap_node *b)
{
	struct crt_rpc_priv	*rpc_priv_a;
	struct crt_rpc_priv	*rpc_priv_b;

	D_ASSERT(a != NULL);
	D_ASSERT(b != NULL);

	rpc_priv_a = container_of(a, struct crt_rpc_priv, crp_timeout_bp_node);
	rpc_priv_b = container_of(b, struct crt_rpc_priv, crp_timeout_bp_node);

	return rpc_priv_a->crp_timeout_ts < rpc_priv_b->crp_timeout_ts;
}

struct d_binheap_ops crt_timeout_bh_ops = {
	.hop_enter	= timeout_bp_node_enter,
	.hop_exit	= timeout_bp_node_exit,
	.hop_compare	= timeout_bp_node_cmp
};