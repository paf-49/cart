/* Copyright (C) 2019-2020 Intel Corporation
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
 * 4. All publications or advertising materials mentioning features or use of
 *    this software are asked, but not required, to acknowledge that it was
 *    developed by Intel Corporation and credit the contributors.
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
 */
/**
 * This file is part of CaRT. It implements the SWIM integration APIs.
 */
#define D_LOGFAC	DD_FAC(swim)
#define CRT_USE_GURT_FAC

#include "crt_internal.h"

#define CRT_OPC_SWIM_PROTO	0xFE000000U
#define CRT_OPC_SWIM_VERSION	0

#define crt_proc_swim_id_t	crt_proc_uint64_t

#define CRT_ISEQ_RPC_SWIM	/* input fields */		 \
	((swim_id_t)		     (src)		CRT_VAR) \
	((struct swim_member_update) (upds)		CRT_ARRAY)

#define CRT_OSEQ_RPC_SWIM	/* output fields */		 \
	((int32_t)		     (rc)		CRT_VAR)

static inline int
crt_proc_struct_swim_member_update(crt_proc_t proc,
				   struct swim_member_update *data)
{
	int rc;

	rc = crt_proc_memcpy(proc, data, sizeof(*data));
	if (rc != 0)
		return -DER_HG;

	return 0;
}

/* One way RPC */
CRT_RPC_DECLARE(crt_rpc_swim, CRT_ISEQ_RPC_SWIM, /* empty */)
CRT_RPC_DEFINE(crt_rpc_swim,  CRT_ISEQ_RPC_SWIM, /* empty */)

/* RPC with acknowledge */
CRT_RPC_DECLARE(crt_rpc_swim_wack, CRT_ISEQ_RPC_SWIM, CRT_OSEQ_RPC_SWIM)
CRT_RPC_DEFINE(crt_rpc_swim_wack,  CRT_ISEQ_RPC_SWIM, CRT_OSEQ_RPC_SWIM)

static void crt_swim_srv_cb(crt_rpc_t *rpc_req);

static struct crt_proto_rpc_format crt_swim_proto_rpc_fmt[] = {
	{
		.prf_flags	= CRT_RPC_FEAT_QUEUE_FRONT |
				  CRT_RPC_FEAT_NO_REPLY,
		.prf_req_fmt	= &CQF_crt_rpc_swim,
		.prf_hdlr	= crt_swim_srv_cb,
		.prf_co_ops	= NULL,
	}, {
		.prf_flags	= CRT_RPC_FEAT_QUEUE_FRONT,
		.prf_req_fmt	= &CQF_crt_rpc_swim_wack,
		.prf_hdlr	= crt_swim_srv_cb,
		.prf_co_ops	= NULL,
	}
};

static struct crt_proto_format crt_swim_proto_fmt = {
	.cpf_name	= "swim-proto",
	.cpf_ver	= CRT_OPC_SWIM_VERSION,
	.cpf_count	= ARRAY_SIZE(crt_swim_proto_rpc_fmt),
	.cpf_prf	= crt_swim_proto_rpc_fmt,
	.cpf_base	= CRT_OPC_SWIM_PROTO,
};

static void crt_swim_srv_cb(crt_rpc_t *rpc_req)
{
	struct crt_grp_priv	*grp_priv = crt_gdata.cg_grp->gg_primary_grp;
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	struct swim_context	*ctx = csm->csm_ctx;
	struct crt_rpc_swim_in	*rpc_swim_input = crt_req_get(rpc_req);
	struct crt_rpc_swim_wack_out *rpc_swim_output = crt_reply_get(rpc_req);
	swim_id_t		 self_id = swim_self_get(ctx);
	int			 rc = -ENOTCONN;

	D_ASSERT(crt_is_service());

	D_TRACE_DEBUG(DB_TRACE, rpc_req,
		"incoming opc %#x with %zu updates %lu <= %lu\n",
		rpc_req->cr_opc, rpc_swim_input->upds.ca_count,
		self_id, rpc_swim_input->src);

	if (self_id != SWIM_ID_INVALID) {
		rc = swim_parse_message(ctx, rpc_swim_input->src,
					rpc_swim_input->upds.ca_arrays,
					rpc_swim_input->upds.ca_count);
		if (rc == -ESHUTDOWN)
			swim_self_set(ctx, SWIM_ID_INVALID);
		else if (rc)
			D_ERROR("swim_parse_message() failed rc=%d\n", rc);
	}

	if (rpc_req->cr_opc & 0xFFFF) { /* RPC with acknowledge? */
		D_TRACE_DEBUG(DB_TRACE, rpc_req,
			"reply to opc %#x with %zu updates %lu <= %lu rc=%d\n",
			rpc_req->cr_opc, rpc_swim_input->upds.ca_count,
			self_id, rpc_swim_input->src, rc);

		rpc_swim_output->rc = rc;
		rc = crt_reply_send(rpc_req);
		if (rc)
			D_ERROR("send reply %d failed: rc=%d\n",
				rpc_swim_output->rc, rc);
	}
}

static int crt_swim_get_member_state(struct swim_context *ctx, swim_id_t id,
				     struct swim_member_state *state);
static int crt_swim_set_member_state(struct swim_context *ctx, swim_id_t id,
				     struct swim_member_state *state);

static void crt_swim_cli_cb(const struct crt_cb_info *cb_info)
{
	struct swim_context	*ctx = cb_info->cci_arg;
	crt_rpc_t		*rpc_req = cb_info->cci_rpc;
	struct crt_rpc_swim_in	*rpc_swim_input = crt_req_get(rpc_req);
	struct swim_member_state id_state;
	swim_id_t		 id = rpc_req->cr_ep.ep_rank;
	int			 rc;

	D_TRACE_DEBUG(DB_TRACE, rpc_req,
		"complete opc %#x with %zu updates %lu => %lu rc=%d\n",
		rpc_req->cr_opc, rpc_swim_input->upds.ca_count,
		rpc_swim_input->src, id, cb_info->cci_rc);

	/* check for RPC with acknowledge a return code of request */
	if (cb_info->cci_rc && (rpc_req->cr_opc & 0xFFFF)) {
		rc = crt_swim_get_member_state(ctx, id, &id_state);
		if (!rc) {
			D_DEBUG(DB_TRACE, "member {%lu I %lu} => {%lu D %lu}",
				id, id_state.sms_incarnation,
				id, (id_state.sms_incarnation + 1));
			id_state.sms_incarnation++;
			id_state.sms_status = SWIM_MEMBER_DEAD;
			crt_swim_set_member_state(ctx, id, &id_state);
		}
	}

	D_FREE(rpc_swim_input->upds.ca_arrays);
}

static int crt_swim_send_message(struct swim_context *ctx, swim_id_t to,
				 struct swim_member_update *upds, size_t nupds)
{
	struct crt_grp_priv	*grp_priv = swim_data(ctx);
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	struct crt_rpc_swim_in	*rpc_swim_input;
	crt_context_t		 crt_ctx;
	crt_rpc_t		*rpc_req;
	crt_endpoint_t		 ep;
	crt_opcode_t		 opc;
	int			 opc_idx = 0;
	int			 ctx_idx = csm->csm_crt_ctx_idx;
	int			 rc;

	crt_ctx = crt_context_lookup(ctx_idx);
	if (crt_ctx == CRT_CONTEXT_NULL) {
		D_ERROR("crt_context_lookup failed\n");
		D_GOTO(out, rc = -DER_UNINIT);
	}

	ep.ep_grp  = &grp_priv->gp_pub;
	ep.ep_rank = (d_rank_t)to;
	ep.ep_tag  = ctx_idx;

	if (nupds > 0 && upds[0].smu_state.sms_status == SWIM_MEMBER_INACTIVE)
		opc_idx = 1;

	opc = CRT_PROTO_OPC(CRT_OPC_SWIM_PROTO, CRT_OPC_SWIM_VERSION, opc_idx);
	rc = crt_req_create(crt_ctx, &ep, opc, &rpc_req);
	if (rc) {
		D_ERROR("crt_req_create() failed rc=%d\n", rc);
		D_GOTO(out, rc);
	}

	if (opc_idx == 0) { /* set timeout for one way RPC only */
		rc = crt_req_set_timeout(rpc_req, CRT_SWIM_RPC_TIMEOUT);
		if (rc) {
			D_TRACE_ERROR(rpc_req,
				"crt_req_set_timeout() failed rc=%d\n", rc);
			crt_req_decref(rpc_req);
			D_GOTO(out, rc);
		}
	}

	rpc_swim_input = crt_req_get(rpc_req);
	rpc_swim_input->src = swim_self_get(ctx);
	rpc_swim_input->upds.ca_arrays = upds;
	rpc_swim_input->upds.ca_count  = nupds;

	D_TRACE_DEBUG(DB_TRACE, rpc_req,
		"sending opc %#x with %zu updates %lu => %lu\n",
		opc, nupds, swim_self_get(ctx), to);

	rc = crt_req_send(rpc_req, crt_swim_cli_cb, ctx);
	if (rc) {
		D_TRACE_ERROR(rpc_req, "crt_req_send() failed rc=%d\n", rc);
		D_GOTO(out, rc);
	}
out:
	return rc;
}

static swim_id_t crt_swim_get_dping_target(struct swim_context *ctx)
{
	struct crt_grp_priv	*grp_priv = swim_data(ctx);
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	swim_id_t		 self_id = swim_self_get(ctx);
	swim_id_t		 id;
	uint32_t		 count = 0;

	D_ASSERT(csm->csm_target != NULL);

	D_SPIN_LOCK(&csm->csm_lock);
	do {
		if (count++ > grp_priv->gp_size) /* don't have a candidate */
			D_GOTO(out, id = SWIM_ID_INVALID);
		/*
		 * Iterate over circled list. So, when a last member is reached
		 * then transparently go to a first and continue.
		 */
		csm->csm_target = D_CIRCLEQ_LOOP_NEXT(&csm->csm_head,
						     csm->csm_target, cst_link);
		id = csm->csm_target->cst_id;
	} while (id == self_id ||
		 csm->csm_target->cst_state.sms_status == SWIM_MEMBER_DEAD);
out:
	D_SPIN_UNLOCK(&csm->csm_lock);
	D_DEBUG(DB_TRACE, "select dping target: %lu => %lu\n", self_id, id);
	return id;
}

static swim_id_t crt_swim_get_iping_target(struct swim_context *ctx)
{
	struct crt_grp_priv	*grp_priv = swim_data(ctx);
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	swim_id_t		 self_id = swim_self_get(ctx);
	swim_id_t		 id;
	uint32_t		 count = 0;

	D_ASSERT(csm->csm_target != NULL);

	D_SPIN_LOCK(&csm->csm_lock);
	do {
		if (count++ > grp_priv->gp_size) /* don't have a candidate */
			D_GOTO(out, id = SWIM_ID_INVALID);
		/*
		 * Iterate over circled list. So, when a last member is reached
		 * then transparently go to a first and continue.
		 */
		csm->csm_target = D_CIRCLEQ_LOOP_NEXT(&csm->csm_head,
						     csm->csm_target, cst_link);
		id = csm->csm_target->cst_id;
	} while (id == self_id ||
		 csm->csm_target->cst_state.sms_status != SWIM_MEMBER_ALIVE);
out:
	D_SPIN_UNLOCK(&csm->csm_lock);
	D_DEBUG(DB_TRACE, "select iping target: %lu => %lu\n", self_id, id);
	return id;
}

static void
crt_swim_notify_rank_state(d_rank_t rank, struct swim_member_state *state)
{
	struct crt_event_cb_priv *cb_priv;
	enum crt_event_type	 cb_type;
	crt_event_cb		 cb_func;
	void			*cb_arg;

	D_ASSERT(state != NULL);
	switch (state->sms_status) {
	case SWIM_MEMBER_ALIVE:
		cb_type = CRT_EVT_ALIVE;
		break;
	case SWIM_MEMBER_DEAD:
		cb_type = CRT_EVT_DEAD;
		break;
	default:
		return;
	}

	/* walk the global list to execute the user callbacks */
	D_RWLOCK_RDLOCK(&crt_plugin_gdata.cpg_event_rwlock);
	d_list_for_each_entry(cb_priv, &crt_plugin_gdata.cpg_event_cbs,
			      cecp_link) {
		cb_func = cb_priv->cecp_func;
		cb_arg  = cb_priv->cecp_args;
		cb_func(rank, CRT_EVS_SWIM, cb_type, cb_arg);
	}
	D_RWLOCK_UNLOCK(&crt_plugin_gdata.cpg_event_rwlock);
}

static int crt_swim_get_member_state(struct swim_context *ctx,
				     swim_id_t id,
				     struct swim_member_state *state)
{
	struct crt_grp_priv	*grp_priv = swim_data(ctx);
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	struct crt_swim_target	*cst;
	int			 rc = -DER_NONEXIST;

	D_SPIN_LOCK(&csm->csm_lock);
	D_CIRCLEQ_FOREACH(cst, &csm->csm_head, cst_link) {
		if (cst->cst_id == id) {
			*state = cst->cst_state;
			rc = 0;
			break;
		}
	}
	D_SPIN_UNLOCK(&csm->csm_lock);

	return rc;
}

static int crt_swim_set_member_state(struct swim_context *ctx,
				     swim_id_t id,
				     struct swim_member_state *state)
{
	struct crt_grp_priv	*grp_priv = swim_data(ctx);
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	struct crt_swim_target	*cst;
	int			 rc = -DER_NONEXIST;

	D_SPIN_LOCK(&csm->csm_lock);
	D_CIRCLEQ_FOREACH(cst, &csm->csm_head, cst_link) {
		if (cst->cst_id == id) {
			cst->cst_state = *state;
			rc = 0;
			break;
		}
	}
	D_SPIN_UNLOCK(&csm->csm_lock);

	if (rc == 0)
		crt_swim_notify_rank_state((d_rank_t)id, state);

	return rc;
}

static void crt_swim_progress_cb(crt_context_t crt_ctx, void *arg)
{
	struct crt_grp_priv	*grp_priv = crt_gdata.cg_grp->gg_primary_grp;
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	struct swim_context	*ctx = csm->csm_ctx;
	swim_id_t		 self_id = swim_self_get(ctx);
	int			 rc;

	if (self_id == SWIM_ID_INVALID)
		return;

	rc = swim_progress(ctx, CRT_SWIM_PROGRESS_TIMEOUT);
	if (rc == -ESHUTDOWN) {
		swim_self_set(ctx, SWIM_ID_INVALID);
	} else if (rc && rc != -ETIMEDOUT) {
		D_ERROR("swim_progress() failed rc=%d\n", rc);
	}
}

void crt_swim_fini(void)
{
	struct crt_grp_priv	*grp_priv = crt_gdata.cg_grp->gg_primary_grp;
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;

	if (!crt_gdata.cg_swim_inited)
		return;

	crt_swim_rank_del_all(grp_priv);

	if (csm->csm_ctx != NULL) {
		if (csm->csm_crt_ctx_idx != -1) {
			crt_unregister_progress_cb(crt_swim_progress_cb,
						   csm->csm_crt_ctx_idx, NULL);
		}
		csm->csm_crt_ctx_idx = -1;
		swim_fini(csm->csm_ctx);
		csm->csm_ctx = NULL;
	}

	crt_gdata.cg_swim_inited = 0;
}

static struct swim_ops crt_swim_ops = {
	.send_message     = &crt_swim_send_message,
	.get_dping_target = &crt_swim_get_dping_target,
	.get_iping_target = &crt_swim_get_iping_target,
	.get_member_state = &crt_swim_get_member_state,
	.set_member_state = &crt_swim_set_member_state,
};

int crt_swim_init(int crt_ctx_idx)
{
	struct crt_grp_priv	*grp_priv = crt_gdata.cg_grp->gg_primary_grp;
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	d_rank_list_t		*grp_membs;
	d_rank_t		 self = grp_priv->gp_self;
	int			 i, rc;


	if (crt_gdata.cg_swim_inited) {
		D_ERROR("Swim already initialized\n");
		D_GOTO(out, rc = -DER_ALREADY);
	}

	grp_membs = grp_priv_get_membs(grp_priv);
	csm->csm_crt_ctx_idx = crt_ctx_idx;
	csm->csm_ctx = swim_init(SWIM_ID_INVALID, &crt_swim_ops, grp_priv);
	if (csm->csm_ctx == NULL) {
		D_ERROR("swim_init() failed for self=%u, crt_ctx_idx=%d\n",
			self, crt_ctx_idx);
		D_GOTO(out, rc = -DER_NOMEM);
	}

	if (self != CRT_NO_RANK && grp_membs != NULL) {
		if (grp_membs->rl_nr != grp_priv->gp_size) {
			D_ERROR("Mismatch in group size. Expected %d got %d\n",
				grp_membs->rl_nr, grp_priv->gp_size);
			D_GOTO(cleanup, rc = -DER_INVAL);
		}

		for (i = 0; i < grp_priv->gp_size; i++) {
			rc = crt_swim_rank_add(grp_priv,
					grp_membs->rl_ranks[i]);
			if (rc) {
				D_ERROR("crt_swim_rank_add() failed=%d\n", rc);
				D_GOTO(cleanup, rc);
			}
		}
	}

	rc = crt_proto_register(&crt_swim_proto_fmt);
	if (rc) {
		D_ERROR("crt_proto_register() failed=%d\n", rc);
		D_GOTO(cleanup, rc);
	}

	rc = crt_register_progress_cb(crt_swim_progress_cb, crt_ctx_idx, NULL);
	if (rc) {
		D_ERROR("crt_register_progress_cb() failed=%d\n", rc);
		D_GOTO(cleanup, rc);
	}
	D_GOTO(out, rc);

cleanup:
	if (self != CRT_NO_RANK && grp_membs != NULL) {
		for (i = 0; i < grp_priv->gp_size; i++)
			crt_swim_rank_del(grp_priv, grp_membs->rl_ranks[i]);
	}
	swim_fini(csm->csm_ctx);
	csm->csm_ctx = NULL;
	csm->csm_crt_ctx_idx = -1;
out:
	if (rc == DER_SUCCESS)
		crt_gdata.cg_swim_inited = 1;
	return rc;
}

int crt_swim_enable(struct crt_grp_priv *grp_priv, int crt_ctx_idx)
{
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	d_rank_t		 self = grp_priv->gp_self;
	swim_id_t		 self_id;
	int			 old_ctx_idx = -1;
	int			 rc = 0;

	if (!crt_initialized() || !crt_is_service())
		D_GOTO(out, rc = 0);

	if (self == CRT_NO_RANK) {
		D_ERROR("Self rank was not set yet\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	if (crt_ctx_idx < 0) {
		D_ERROR("Invalid context index\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	D_SPIN_LOCK(&csm->csm_lock);
	if (csm->csm_crt_ctx_idx != crt_ctx_idx)
		old_ctx_idx = csm->csm_crt_ctx_idx;
	csm->csm_crt_ctx_idx = crt_ctx_idx;
	self_id = swim_self_get(csm->csm_ctx);
	if (self_id != (swim_id_t)self)
		swim_self_set(csm->csm_ctx, (swim_id_t)self);
	D_SPIN_UNLOCK(&csm->csm_lock);

	if (old_ctx_idx != -1) {
		rc = crt_unregister_progress_cb(crt_swim_progress_cb,
						old_ctx_idx, NULL);
		if (rc)
			D_ERROR("crt_unregister_progress_cb() failed: rc=%d\n",
				rc);
	}
	if (old_ctx_idx != crt_ctx_idx) {
		rc = crt_register_progress_cb(crt_swim_progress_cb,
					      crt_ctx_idx, NULL);
		if (rc)
			D_ERROR("crt_register_progress_cb() failed: rc=%d\n",
				rc);
	}

out:
	return rc;
}

int crt_swim_disable(struct crt_grp_priv *grp_priv, int crt_ctx_idx)
{
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	int			 old_ctx_idx = -1;
	int			 rc = -DER_NONEXIST;

	if (!crt_initialized() || !crt_is_service())
		D_GOTO(out, rc = 0);

	if (crt_ctx_idx < 0) {
		swim_self_set(csm->csm_ctx, SWIM_ID_INVALID);
		D_GOTO(out, rc = 0);
	}

	D_SPIN_LOCK(&csm->csm_lock);
	if (csm->csm_crt_ctx_idx == crt_ctx_idx) {
		old_ctx_idx = csm->csm_crt_ctx_idx;
		csm->csm_crt_ctx_idx = -1;
		swim_self_set(csm->csm_ctx, SWIM_ID_INVALID);
	}
	D_SPIN_UNLOCK(&csm->csm_lock);

	if (old_ctx_idx != -1) {
		rc = crt_unregister_progress_cb(crt_swim_progress_cb,
						old_ctx_idx, NULL);
		if (rc)
			D_ERROR("crt_unregister_progress_cb() failed: rc=%d\n",
				rc);
	}

out:
	return rc;
}

void crt_swim_disable_all(void)
{
	struct crt_grp_priv	*grp_priv = crt_gdata.cg_grp->gg_primary_grp;
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	int			 old_ctx_idx;
	int			 rc;

	if (!crt_initialized() || !crt_is_service())
		return;

	D_SPIN_LOCK(&csm->csm_lock);
	old_ctx_idx = csm->csm_crt_ctx_idx;
	csm->csm_crt_ctx_idx = -1;
	swim_self_set(csm->csm_ctx, SWIM_ID_INVALID);
	D_SPIN_UNLOCK(&csm->csm_lock);

	if (old_ctx_idx != -1) {
		rc = crt_unregister_progress_cb(crt_swim_progress_cb,
						old_ctx_idx, NULL);
		if (rc)
			D_ERROR("crt_unregister_progress_cb() failed: rc=%d\n",
				rc);
	}
}

int crt_swim_rank_add(struct crt_grp_priv *grp_priv, d_rank_t rank)
{
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	struct crt_swim_target	*cst = NULL;
	swim_id_t		 self_id;
	d_rank_t		 self = grp_priv->gp_self;
	bool			 self_in_list = false;
	bool			 rank_in_list = false;
	int			 n, rc = 0;

	D_ASSERT(crt_initialized());

	if (!crt_is_service())
		D_GOTO(out, rc = 0);

	if (self == CRT_NO_RANK) {
		D_ERROR("Self rank was not set yet\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	D_ALLOC_PTR(cst);
	if (cst == NULL)
		D_GOTO(out, rc = -DER_NOMEM);

	D_SPIN_LOCK(&csm->csm_lock);
	if (D_CIRCLEQ_EMPTY(&csm->csm_head)) {
		cst->cst_id = (swim_id_t)self;
		cst->cst_state.sms_incarnation = 0;
		cst->cst_state.sms_status = SWIM_MEMBER_ALIVE;
		D_CIRCLEQ_INSERT_HEAD(&csm->csm_head, cst, cst_link);
		self_in_list = true;

		csm->csm_target = cst;

		D_DEBUG(DB_TRACE, "add self {%lu %c %lu}\n", cst->cst_id,
			SWIM_STATUS_CHARS[cst->cst_state.sms_status],
			cst->cst_state.sms_incarnation);

		cst = NULL;
	}

	if (rank != self) {
		if (cst == NULL) {
			D_ALLOC_PTR(cst);
			if (cst == NULL)
				D_GOTO(out_unlock, rc = -DER_NOMEM);
		}
		cst->cst_id = (swim_id_t)rank;
		cst->cst_state.sms_incarnation = 0;
		cst->cst_state.sms_status = SWIM_MEMBER_INACTIVE;
		D_CIRCLEQ_INSERT_AFTER(&csm->csm_head, csm->csm_target, cst,
					cst_link);
		rank_in_list = true;

		for (n = 1 + rand() % (grp_priv->gp_size + 1); n > 0; n--)
			csm->csm_target = D_CIRCLEQ_LOOP_NEXT(&csm->csm_head,
							      csm->csm_target,
							      cst_link);

		D_DEBUG(DB_TRACE, "add member {%lu %c %lu}\n", cst->cst_id,
			SWIM_STATUS_CHARS[cst->cst_state.sms_status],
			cst->cst_state.sms_incarnation);
		cst = NULL;
	}

	self_id = swim_self_get(csm->csm_ctx);
	if (self_id != (swim_id_t)self)
		swim_self_set(csm->csm_ctx, (swim_id_t)self);

out_unlock:
	D_SPIN_UNLOCK(&csm->csm_lock);
out:
	if (cst != NULL)
		D_FREE(cst);

	if (rc) {
		if (rank_in_list)
			crt_swim_rank_del(grp_priv, rank);
		if (self_in_list)
			crt_swim_rank_del(grp_priv, self);
	}
	return rc;
}

int crt_swim_rank_del(struct crt_grp_priv *grp_priv, d_rank_t rank)
{
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	struct crt_swim_target	*cst, *next;
	int			 rc = -DER_NONEXIST;

	D_ASSERT(crt_initialized());

	if (!crt_is_service())
		D_GOTO(out, rc = 0);

	D_SPIN_LOCK(&csm->csm_lock);
	D_CIRCLEQ_FOREACH(cst, &csm->csm_head, cst_link) {
		if (cst->cst_id == (swim_id_t)rank) {
			D_DEBUG(DB_TRACE, "del member {%lu %c %lu}\n",
				cst->cst_id,
				SWIM_STATUS_CHARS[cst->cst_state.sms_status],
				cst->cst_state.sms_incarnation);

			next = D_CIRCLEQ_LOOP_NEXT(&csm->csm_head,
						   csm->csm_target, cst_link);
			D_CIRCLEQ_REMOVE(&csm->csm_head, cst, cst_link);
			if (D_CIRCLEQ_EMPTY(&csm->csm_head)) {
				swim_self_set(csm->csm_ctx, SWIM_ID_INVALID);
				csm->csm_target = NULL;
			} else if (csm->csm_target == cst) {
				csm->csm_target = next;
			}

			rc = 0;
			break;
		}
	}
	D_SPIN_UNLOCK(&csm->csm_lock);

	if (rc == 0)
		D_FREE(cst);
out:
	return rc;
}

void crt_swim_rank_del_all(struct crt_grp_priv *grp_priv)
{
	struct crt_swim_membs	*csm = &grp_priv->gp_membs_swim;
	struct crt_swim_target	*cst;

	if (!crt_initialized() || !crt_is_service())
		return;

	D_SPIN_LOCK(&csm->csm_lock);
	swim_self_set(csm->csm_ctx, SWIM_ID_INVALID);
	csm->csm_target = NULL;
	while (!D_CIRCLEQ_EMPTY(&csm->csm_head)) {
		cst = D_CIRCLEQ_FIRST(&csm->csm_head);
		D_DEBUG(DB_TRACE, "del member {%lu %c %lu}\n", cst->cst_id,
			SWIM_STATUS_CHARS[cst->cst_state.sms_status],
			cst->cst_state.sms_incarnation);
		D_CIRCLEQ_REMOVE(&csm->csm_head, cst, cst_link);
		D_FREE(cst);
	}
	D_SPIN_UNLOCK(&csm->csm_lock);
}

int
crt_rank_state_get(crt_group_t *grp, d_rank_t rank,
		   struct swim_member_state *state)
{
	struct crt_grp_priv	*grp_priv;
	struct crt_swim_membs	*csm;
	int			 rc = 0;

	if (grp == NULL) {
		D_ERROR("Passed group is NULL\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	if (state == NULL) {
		D_ERROR("Passed state pointer is NULL\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	if (rank == CRT_NO_RANK) {
		D_ERROR("Rank is invalid\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	grp_priv = crt_grp_pub2priv(grp);
	if (!grp_priv->gp_primary) {
		D_ERROR("Only available for primary groups\n");
		D_GOTO(out, rc = -DER_INVAL);
	}

	csm = &grp_priv->gp_membs_swim;
	rc = crt_swim_get_member_state(csm->csm_ctx, (swim_id_t)rank, state);

out:
	return rc;
}
