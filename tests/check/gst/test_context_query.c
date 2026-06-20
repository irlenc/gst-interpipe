/* GStreamer
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/check/gstcheck.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

/*
 * A context query travelling upstream into an interpipesrc must be forwarded
 * across the interpipe boundary and answered by the producer pipeline that
 * the connected interpipesink belongs to. This is what lets two decoupled
 * pipelines share a hardware context (e.g. a VADisplay) the same way they
 * already share buffers and events.
 */

#define TEST_CONTEXT_TYPE "interpipe-test-context"

static gboolean context_query_reached_producer = FALSE;

static GstPadProbeReturn
answer_context_query (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  GstQuery *query;
  const gchar *ctx_type = NULL;
  GstContext *ctx;

  query = GST_PAD_PROBE_INFO_QUERY (info);
  if (GST_QUERY_TYPE (query) != GST_QUERY_CONTEXT)
    return GST_PAD_PROBE_PASS;

  gst_query_parse_context_type (query, &ctx_type);
  if (g_strcmp0 (ctx_type, TEST_CONTEXT_TYPE) != 0)
    return GST_PAD_PROBE_PASS;

  /* The query made it into the producer pipeline: answer it. */
  ctx = gst_context_new (TEST_CONTEXT_TYPE, TRUE);
  gst_query_set_context (query, ctx);
  gst_context_unref (ctx);
  context_query_reached_producer = TRUE;

  return GST_PAD_PROBE_HANDLED;
}

GST_START_TEST (context_query_forwarding)
{
  GstPipeline *sink_pipe;
  GstPipeline *src_pipe;
  GstElement *intersrc;
  GstElement *producer;
  GstPad *producer_pad;
  GstPad *srcpad;
  GstQuery *query;
  GstContext *ctx = NULL;
  GError *error = NULL;

  /* Producer pipeline: the producer element feeds the interpipesink. */
  sink_pipe = GST_PIPELINE (gst_parse_launch
      ("fakesrc name=producer ! interpipesink name=isink async=false sync=false",
          &error));
  fail_if (error);
  producer = gst_bin_get_by_name (GST_BIN (sink_pipe), "producer");
  fail_if (!producer);

  /* Consumer pipeline: an interpipesrc listening to the producer's node. */
  src_pipe = GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=isrc listen-to=isink ! fakesink async=false sync=false",
          &error));
  fail_if (error);
  intersrc = gst_bin_get_by_name (GST_BIN (src_pipe), "isrc");
  fail_if (!intersrc);

  /* Answer the context query once it is forwarded upstream to the producer. */
  producer_pad = gst_element_get_static_pad (producer, "src");
  fail_if (!producer_pad);
  gst_pad_add_probe (producer_pad, GST_PAD_PROBE_TYPE_QUERY_UPSTREAM,
      answer_context_query, NULL, NULL);

  /* Bring both pipelines up so the interpipe link is established. */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink_pipe), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (sink_pipe), NULL, NULL,
          GST_CLOCK_TIME_NONE));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src_pipe), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (src_pipe), NULL, NULL,
          GST_CLOCK_TIME_NONE));

  /* Send a context query into the interpipesrc src pad. */
  srcpad = GST_BASE_SRC_PAD (G_OBJECT (intersrc));
  fail_if (!srcpad);
  query = gst_query_new_context (TEST_CONTEXT_TYPE);

  fail_unless (gst_pad_query (srcpad, query),
      "interpipesrc did not answer the forwarded context query");
  fail_unless (context_query_reached_producer,
      "context query was not forwarded across the interpipe boundary");
  gst_query_parse_context (query, &ctx);
  fail_unless (ctx != NULL,
      "no context returned from the producer pipeline");

  gst_query_unref (query);

  /* Teardown */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (src_pipe), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink_pipe), GST_STATE_NULL));

  gst_object_unref (producer_pad);
  g_object_unref (producer);
  g_object_unref (intersrc);
  g_object_unref (sink_pipe);
  g_object_unref (src_pipe);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc1 = tcase_create ("context_query_forwarding");

  suite_add_tcase (suite, tc1);
  tcase_add_test (tc1, context_query_forwarding);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
