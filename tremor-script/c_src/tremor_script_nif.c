#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdbool.h>
#include <string.h>
#include <dirent.h>

#include "ei.h"
#include "erl_nif.h"
#include "ts.h"

static ErlNifResourceType *ts_RESOURCE = NULL;

typedef struct
{
} ts_handle;

static ERL_NIF_TERM ts_eval(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[]);

static ErlNifFunc nif_funcs[] =
    {
        {"eval", 1, ts_eval}};

static ERL_NIF_TERM ts_eval(ErlNifEnv *env, int argc,
                            const ERL_NIF_TERM argv[])
{
    ErlNifBinary ast, result;
    if (!enif_inspect_binary(env, argv[0], &ast))
    {
        return enif_make_badarg(env);
    }
    unsigned char *ast_c_str = malloc(ast.size + 1);
    if (!ast_c_str)
    {
        return enif_make_atom(env, "error");
    };
    memcpy(ast_c_str, ast.data, ast.size);
    ast_c_str[ast.size] = 0;

    char json_value[8192];
    json_value[0] = 0;
    tremor_script_c_eval(ast_c_str, json_value, 8192);
    result.size = strnlen(json_value, 8192);
    enif_alloc_binary(result.size, &result);
    memcpy(result.data, json_value, result.size);
    free(ast_c_str);
    return enif_make_binary(env, &result);
}

static void ts_resource_cleanup(ErlNifEnv *env, void *arg)
{
    /* Delete any dynamically allocated memory stored in ts_handle */
    /* ts_handle* handle = (ts_handle*)arg; */
}

static int on_load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
    ErlNifResourceFlags flags = ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER;
    ErlNifResourceType *rt = enif_open_resource_type(env, NULL, "ts_resource", &ts_resource_cleanup, flags, NULL);

    if (rt == NULL)
        return -1;
    ts_RESOURCE = rt;
    return 0;
}

ERL_NIF_INIT(ts, nif_funcs, &on_load, NULL, NULL, NULL);
