/* Copyright (C) 2019 Intel Corporation
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
 * This is a cart multi provider test based on crt API.
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>
#include <assert.h>
#include <getopt.h>
#include <semaphore.h>

#include <gurt/common.h>
#include <gurt/fault_inject.h>
#include <cart/api.h>
#include "test_group_rpc.h"
#include "tests_common.h"

#define TEST_MULTI_PROV_BASE		0x010000000
#define TEST_MULTI_PROV_VER		0
#define TEST_CTX_MAX_NUM		(72)
#define DEFAULT_PROGRESS_CTX_IDX	0

#define NUM_SERVER_CTX 4

#define DBG_PRINT(x...)							\
	do {								\
		if (test_g.t_is_service)				\
			fprintf(stderr, "SRV [rank=%d pid=%d]\t",	\
			test_g.t_my_rank,				\
			test_g.t_my_pid);				\
		else							\
			fprintf(stderr, "CLI [rank=%d pid=%d]\t",	\
			test_g.t_my_rank,				\
			test_g.t_my_pid);				\
		fprintf(stderr, x);					\
	} while (0)

struct test_t {
	crt_group_t	*t_local_group;
	crt_group_t	*t_remote_group;
	char		*t_local_group_name;
	char		*t_remote_group_name;
	uint32_t	 t_remote_group_size;
	d_rank_t	 t_my_rank;
	int32_t		 t_my_pid;
	char		*t_grp_cfg;
	uint32_t	 t_should_attach:1,
			 t_shutdown:1;
	int		 t_is_service;
	int		 t_infinite_loop;
	int		 t_hold;
	int		 t_shut_only;
	uint32_t	 t_hold_time;
	unsigned int	 t_ctx_num;
	crt_context_t	 t_crt_ctx[TEST_CTX_MAX_NUM];
	int		 t_thread_id[TEST_CTX_MAX_NUM]; /* logical tid */
	pthread_t	 t_tid[TEST_CTX_MAX_NUM];
	sem_t		 t_token_to_proceed;
	int		 t_roomno;
	struct d_fault_attr_t	 *t_fault_attr_1000;
	struct d_fault_attr_t	 *t_fault_attr_5000;
};

struct test_t test_g = {
	.t_shutdown = 0,
	.t_hold_time = 0,
	.t_ctx_num = 1,
	.t_roomno = 1082
};

static inline void
test_sem_timedwait(sem_t *sem, int sec, int line_number)
{
	struct timespec			deadline;
	int				rc;

	rc = clock_gettime(CLOCK_REALTIME, &deadline);
	D_ASSERTF(rc == 0, "clock_gettime() failed at line %d rc: %d\n",
		  line_number, rc);
	deadline.tv_sec += sec;
	rc = sem_timedwait(sem, &deadline);
	D_ASSERTF(rc == 0, "sem_timedwait() failed at line %d rc: %d\n",
		  line_number, rc);
}

void
client_cb_common(const struct crt_cb_info *cb_info)
{
	crt_rpc_t			*rpc_req;
	struct test_ping_check_in	*rpc_req_input;
	struct test_ping_check_out	*rpc_req_output;

	rpc_req = cb_info->cci_rpc;

	if (cb_info->cci_arg != NULL)
		*(int *) cb_info->cci_arg = 1;

	switch (cb_info->cci_rpc->cr_opc) {
	case TEST_OPC_CHECKIN:
		rpc_req_input = crt_req_get(rpc_req);
		if (rpc_req_input == NULL)
			return;
		rpc_req_output = crt_reply_get(rpc_req);
		if (rpc_req_output == NULL)
			return;
		if (cb_info->cci_rc != 0) {
			D_ERROR("rpc (opc: %#x) failed, rc: %d.\n",
				rpc_req->cr_opc, cb_info->cci_rc);
			D_FREE(rpc_req_input->name);
			break;
		}
		DBG_PRINT("%s checkin result - ret: %d, room_no: %d, "
		       "bool_val %d.\n",
		       rpc_req_input->name, rpc_req_output->ret,
		       rpc_req_output->room_no, rpc_req_output->bool_val);
		D_FREE(rpc_req_input->name);
		sem_post(&test_g.t_token_to_proceed);
		D_ASSERT(rpc_req_output->bool_val == true);
		break;
	case TEST_OPC_SHUTDOWN:
		test_g.t_shutdown = 1;
		sem_post(&test_g.t_token_to_proceed);
		break;
	default:
		break;
	}
}

static void *
progress_func(void *arg)
{
	crt_context_t	ctx;
	pthread_t	current_thread = pthread_self();
	int		num_cores = sysconf(_SC_NPROCESSORS_ONLN);
	cpu_set_t	cpuset;
	int		t_idx;
	int		rc;

	t_idx = *(int *)arg;
	CPU_ZERO(&cpuset);
	CPU_SET(t_idx % num_cores, &cpuset);
	pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);

	DBG_PRINT("progress thread %d running on core %d...\n",
		t_idx, sched_getcpu());

	ctx = (crt_context_t)test_g.t_crt_ctx[t_idx];
	/* progress loop */
	while (1) {
		rc = crt_progress(ctx, 0, NULL, NULL);
		if (rc != 0 && rc != -DER_TIMEDOUT)
			D_ERROR("crt_progress failed rc: %d.\n", rc);
		if (test_g.t_shutdown == 1)
			break;
	};

	DBG_PRINT("%s: rc: %d, test_srv.do_shutdown: %d.\n", __func__, rc,
	       test_g.t_shutdown);
	DBG_PRINT("%s: progress thread exit ...\n", __func__);

	return NULL;
}

static struct crt_proto_rpc_format my_proto_rpc_fmt_test_multi_prov_cli[] = {
	{
		.prf_req_fmt	= &CQF_test_ping_check,
	}, {
		.prf_flags	= CRT_RPC_FEAT_NO_REPLY,
	}, {
		.prf_flags	= CRT_RPC_FEAT_NO_TIMEOUT,
		.prf_req_fmt	= &CQF_crt_test_ping_delay,
	}
};

static struct crt_proto_format my_proto_fmt_test_multi_prov_cli = {
	.cpf_name = "my-proto-test-cli",
	.cpf_ver = TEST_MULTI_PROV_VER,
	.cpf_count = ARRAY_SIZE(my_proto_rpc_fmt_test_multi_prov_cli),
	.cpf_prf = &my_proto_rpc_fmt_test_multi_prov_cli[0],
	.cpf_base = TEST_MULTI_PROV_BASE,
};

#define NUM_ATTACH_RETRIES 10
void
test_init(void)
{
	sleep(3);
	uint32_t	flag;
	int		i;
//	char			*grp_cfg_file;
	d_rank_list_t		*rank_list;
	int			 attach_retries_left;
	int		rc = 0;

	DBG_PRINT("remote group: %s\n", test_g.t_remote_group_name);

	/* In order to use things like D_ASSERTF, logging needs to be active
	 * even if cart is not
	 */
	rc = d_log_init();
	D_ASSERT(rc == 0);

	D_DEBUG(DB_TEST, "starting multi-prov test\n");
	rc = sem_init(&test_g.t_token_to_proceed, 0, 0);
	D_ASSERTF(rc == 0, "sem_init() failed.\n");

	flag = CRT_FLAG_BIT_PMIX_DISABLE | CRT_FLAG_BIT_LM_DISABLE;
	// crt_init() reads OIF_INTERFACE, PHY_ADDR, and OFI_PORT. Currently
	// step 1
	rc = crt_init(test_g.t_local_group_name, flag);
	D_ASSERTF(rc == 0, "crt_init() failed, rc: %d\n", rc);

	test_g.t_fault_attr_1000 = d_fault_attr_lookup(1000);
	test_g.t_fault_attr_5000 = d_fault_attr_lookup(5000);

	test_g.t_grp_cfg = getenv("CRT_L_GRP_CFG");
	/* register RPCs */
	rc = crt_proto_register(&my_proto_fmt_test_multi_prov_cli);
	D_ASSERTF(rc == 0, "crt_proto_register() failed. rc: %d\n", rc);

	attach_retries_left = NUM_ATTACH_RETRIES;

	while (attach_retries_left-- > 0) {
		rc = crt_group_attach_v2(test_g.t_remote_group_name,
                &test_g.t_remote_group);
		if (rc == 0)
			break;

		DBG_PRINT("attach failed (rc=%d). retries left %d\n",
			rc, attach_retries_left);
		sleep(1);
	}
	assert(rc == 0);
if (0) {

	// attach step 2
	rc = crt_group_view_create("server_grp", &test_g.t_remote_group);
	if (!test_g.t_remote_group || rc != 0) {
		D_ERROR("Failed to create group view; rc=%d\n", rc);
		assert(0);
	}
}

	D_DEBUG(DB_TEST, "here.\n");
	// step 3 create context 0
	for (i = 0; i < test_g.t_ctx_num; i++) {
		rc = crt_context_create(&test_g.t_crt_ctx[i]);
		D_ASSERTF(rc == 0, "crt_context_create() failed. rc: %d\n", rc);
		rc = pthread_create(&test_g.t_tid[i], NULL, progress_func,
				    &test_g.t_thread_id[i]);
		D_ASSERTF(rc == 0, "pthread_create() failed. rc: %d\n", rc);
	}

if (0) {
//	grp_cfg_file = getenv("CRT_L_GRP_CFG");
//	rc = tc_load_group_from_file(grp_cfg_file, test_g.t_remote_group, NUM_SERVER_CTX,
//				-1, false);
//	if (rc != 0) {
//		D_ERROR("tc_load_group_from_file() failed; rc=%d\n", rc);
//		assert(0);
//	}
} // if (0) {

	rc = crt_group_size(test_g.t_remote_group, &test_g.t_remote_group_size);
	if (rc != 0) {
		D_ERROR("crt_group_size() failed; rc=%d\n", rc);
		assert(0);
	}
	D_ASSERTF(test_g.t_remote_group_size != 0, "group size can't 0\n");

	rc = crt_group_ranks_get(test_g.t_remote_group, &rank_list);
	if (rc != 0) {
		D_ERROR("crt_group_ranks_get() failed; rc=%d\n", rc);
		assert(0);
	}

    /**
	rc = crt_group_psr_set(test_g.t_remote_group, rank_list->rl_ranks[0]);
	if (rc != 0) {
		D_ERROR("crt_group_psr_set() failed; rc=%d\n", rc);
		assert(0);
	}
    */

	test_g.t_shutdown = 0;
}

void
check_in(crt_group_t *remote_group, int rank)
{
	crt_rpc_t			*rpc_req = NULL;
	struct test_ping_check_in	*rpc_req_input;
	crt_endpoint_t			 server_ep = {0};
	char				*buffer;
	int				 rc;

	server_ep.ep_grp = remote_group;
	server_ep.ep_rank = rank;
	rc = crt_req_create(test_g.t_crt_ctx[0], &server_ep,
			TEST_OPC_CHECKIN, &rpc_req);
	D_ASSERTF(rc == 0 && rpc_req != NULL, "crt_req_create() failed,"
			" rc: %d rpc_req: %p\n", rc, rpc_req);

	rpc_req_input = crt_req_get(rpc_req);
	D_ASSERTF(rpc_req_input != NULL, "crt_req_get() failed."
			" rpc_req_input: %p\n", rpc_req_input);

	/**
	 * example to inject faults to D_ALLOC. To turn it on, edit the fault
	 * config file: under fault id 1000, change the probability from 0 to
	 * anything in [1, 100]
	 */
	if (D_SHOULD_FAIL(test_g.t_fault_attr_1000)) {
		buffer = NULL;
	} else {
		D_ALLOC(buffer, 256);
		D_INFO("not injecting fault.\n");
	}

	D_ASSERTF(buffer != NULL, "Cannot allocate memory.\n");
	snprintf(buffer,  256, "Guest %d", test_g.t_my_rank);
	rpc_req_input->name = buffer;
	rpc_req_input->age = 21;
	rpc_req_input->days = 7;
	rpc_req_input->bool_val = true;
	D_DEBUG(DB_TEST, "client(rank %d tag %d) sending checkin rpc to server "
			"rank %d tag %d, "
			"name: %s, age: %d, days: %d, bool_val %d.\n",
		test_g.t_my_rank, 0, rank, server_ep.ep_tag, rpc_req_input->name,
		rpc_req_input->age, rpc_req_input->days,
		rpc_req_input->bool_val);

	/* send an rpc, print out reply */
	rc = crt_req_send(rpc_req, client_cb_common, NULL);
	D_ASSERTF(rc == 0, "crt_req_send() failed. rc: %d\n", rc);
}

void
test_run(void)
{
	int				ii;

	D_DEBUG(DB_TEST, "test_g.t_remote_group_size %d\n",
			test_g.t_remote_group_size);
	for (ii = 0; ii < test_g.t_remote_group_size; ii++) {
		check_in(test_g.t_remote_group, ii);
	}

	for (ii = 0; ii < test_g.t_remote_group_size; ii++)
		test_sem_timedwait(&test_g.t_token_to_proceed, 61, __LINE__);
}

void
test_fini()
{
	int				 ii;
	crt_endpoint_t			 server_ep = {0};
	crt_rpc_t			*rpc_req = NULL;
	int				 rc = 0;

	/* client rank 0 tells all servers to shut down */
	if (0)
	for (ii = 0; ii < test_g.t_remote_group_size; ii++) {
		server_ep.ep_grp = test_g.t_remote_group;
		server_ep.ep_rank = ii;
		rc = crt_req_create(test_g.t_crt_ctx[0], &server_ep,
				TEST_OPC_SHUTDOWN, &rpc_req);
		D_ASSERTF(rc == 0 && rpc_req != NULL,
				"crt_req_create() failed. "
				"rc: %d, rpc_req: %p\n", rc, rpc_req);
		rc = crt_req_send(rpc_req, client_cb_common, NULL);
		D_ASSERTF(rc == 0, "crt_req_send() failed. rc: %d\n",
				rc);

		test_sem_timedwait(&test_g.t_token_to_proceed, 61,
				__LINE__);
	}
	test_g.t_shutdown = 1;

	rc = crt_group_view_destroy(test_g.t_remote_group);
	if (rc != 0) {
		D_ERROR("crt_group_view_destroy() failed; rc=%d\n", rc);
		assert(0);
	}

	for (ii = 0; ii < test_g.t_ctx_num; ii++) {
		rc = pthread_join(test_g.t_tid[ii], NULL);
		if (rc != 0)
			DBG_PRINT("pthread_join failed. rc: %d\n", rc);
		D_DEBUG(DB_TEST, "joined progress thread.\n");

		/* try to flush indefinitely */
		rc = crt_context_flush(test_g.t_crt_ctx[ii], 0);
		D_ASSERTF(rc == 0 || rc == -DER_TIMEDOUT,
			  "crt_context_flush() failed. rc: %d\n", rc);

		rc = crt_context_destroy(test_g.t_crt_ctx[ii], 0);
		D_ASSERTF(rc == 0, "crt_context_destroy() failed. rc: %d\n",
			  rc);
		D_DEBUG(DB_TEST, "destroyed crt_ctx. id %d\n", ii);
	}

	rc = sem_destroy(&test_g.t_token_to_proceed);
	D_ASSERTF(rc == 0, "sem_destroy() failed.\n");

//	if (test_g.t_is_service) {
//		crt_swim_fini();
//	}

	rc = crt_finalize();
	D_ASSERTF(rc == 0, "crt_finalize() failed. rc: %d\n", rc);

	d_log_fini();

	D_DEBUG(DB_TEST, "exiting.\n");
}

int
test_parse_args(int argc, char **argv)
{
	int				option_index = 0;
	int				rc = 0;
	struct option			long_options[] = {
		{"name", required_argument, 0, 'n'},
		{"attach_to", required_argument, 0, 'a'},
		{"holdtime", required_argument, 0, 'h'},
		{"hold", no_argument, &test_g.t_hold, 1},
		{"is_service", no_argument, &test_g.t_is_service, 1},
		{"ctx_num", required_argument, 0, 'c'},
		{"loop", no_argument, &test_g.t_infinite_loop, 1},
		{"shut_only", no_argument, &test_g.t_shut_only, 1},
		{0, 0, 0, 0}
	};

	while (1) {
		rc = getopt_long(argc, argv, "n:a:c:h:", long_options,
				 &option_index);
		if (rc == -1)
			break;
		switch (rc) {
		case 0:
			if (long_options[option_index].flag != 0)
				break;
		case 'n':
			test_g.t_local_group_name = optarg;
			break;
		case 'a':
			test_g.t_remote_group_name = optarg;
			test_g.t_should_attach = 1;
			break;
		case 'c': {
			unsigned int	nr;
			char		*end;

			nr = strtoul(optarg, &end, 10);
			if (end == optarg || nr == 0 || nr > TEST_CTX_MAX_NUM) {
				DBG_PRINT("invalid ctx_num %d exceed "
					"[%d, %d], using 1 for test.\n", nr,
					1, TEST_CTX_MAX_NUM);
			} else {
				test_g.t_ctx_num = nr;
				DBG_PRINT("cli: will create %d contexts.\n",
					nr);
			}
			break;
		}
		case 'h':
			test_g.t_hold = 1;
			test_g.t_hold_time = atoi(optarg);
			break;
		case '?':
			return 1;
		default:
			return 1;
		}
	}
	if (optind < argc) {
		DBG_PRINT("cli: non-option argv elements encountered.\n");
		return 1;
	}

	return 0;
}

int main(int argc, char **argv)
{

	int	rc;

	rc = test_parse_args(argc, argv);
	if (rc != 0) {
		DBG_PRINT("test_parse_args() failed, rc: %d.\n",
			rc);
		return rc;
	}

	test_init();
	test_run();
	if (test_g.t_hold)
		sleep(test_g.t_hold_time);
	test_fini();

	return rc;
}
