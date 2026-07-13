/* GStreamer
 * Copyright (C) 2026 RidgeRun, LLC (http://www.ridgerun.com)
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

/*
 * An interpipesrc whose downstream advertises an unfixated field must still
 * negotiate once the producer's fixed caps arrive.
 *
 * videocrop is the real-world case: its crop is set at runtime, so it accepts
 * any size and offers a width/height range upstream. When the base source's
 * create loop reaches negotiate() before the producer has primed caps onto the
 * listener, the default negotiate has nothing but that range to work with and
 * the leg dies with not-negotiated ("Internal data stream error") even though
 * the producer is about to deliver perfectly fixed caps.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/check/gstcheck.h>
#include <gst/app/gstappsink.h>

#define PRODUCER_CAPS \
  "video/x-raw,format=(string)I420,width=(int)320,height=(int)240," \
  "framerate=(fraction)30/1"

/* The producer pins fixed caps, so what the leg negotiates never depends on
 * which format videotestsrc happens to list first. */
static GstPipeline *
create_producer (const gchar * node_name)
{
  GstPipeline *producer;
  gchar *desc;
  GError *error = NULL;

  desc = g_strdup_printf ("videotestsrc is-live=true ! " PRODUCER_CAPS " ! "
      "interpipesink name=%s forward-events=true forward-eos=false "
      "sync=false async=false", node_name);
  producer = GST_PIPELINE (gst_parse_launch (desc, &error));
  g_free (desc);
  fail_if (error);

  return producer;
}

/* videocrop sits directly downstream of the interpipesrc, exactly as a BBS
 * scene item leg does. It is what puts an unfixated width/height range in
 * front of the negotiation. */
static GstPipeline *
create_consumer (const gchar * node_name)
{
  GstPipeline *consumer;
  gchar *desc;
  GError *error = NULL;

  desc = g_strdup_printf ("interpipesrc name=isrc listen-to=%s is-live=true "
      "format=time ! videocrop top=0 left=0 right=0 bottom=0 ! videoconvert ! "
      "appsink name=asink async=false sync=false", node_name);
  consumer = GST_PIPELINE (gst_parse_launch (desc, &error));
  g_free (desc);
  fail_if (error);

  return consumer;
}

/* A sample only ever arrives if the leg negotiated. Assert on the caps too, so
 * a leg that negotiated something other than what the producer sends (a
 * fixated-from-range 1x1, say) is a failure rather than a pass. */
static void
assert_producer_caps_reached_appsink (GstPipeline * consumer)
{
  GstElement *asink;
  GstSample *sample;
  GstCaps *caps;
  GstCaps *expected;

  asink = gst_bin_get_by_name (GST_BIN (consumer), "asink");
  fail_unless (asink != NULL);

  sample = gst_app_sink_try_pull_sample (GST_APP_SINK (asink), 5 * GST_SECOND);
  fail_unless (sample != NULL,
      "no buffer reached the appsink: the consumer leg never negotiated");

  caps = gst_sample_get_caps (sample);
  fail_unless (caps != NULL);

  expected = gst_caps_from_string (PRODUCER_CAPS);
  fail_unless (gst_caps_is_always_compatible (caps, expected),
      "leg negotiated %" GST_PTR_FORMAT " instead of the producer caps %"
      GST_PTR_FORMAT, caps, expected);

  gst_caps_unref (expected);
  gst_sample_unref (sample);
  gst_object_unref (asink);
}

static void
assert_no_error_on_bus (GstPipeline * pipeline)
{
  GstMessage *msg;

  msg = gst_bus_pop_filtered (GST_ELEMENT_BUS (GST_ELEMENT (pipeline)),
      GST_MESSAGE_ERROR);
  if (msg) {
    GError *err = NULL;
    gchar *dbg = NULL;

    gst_message_parse_error (msg, &err, &dbg);
    fail ("pipeline posted an error: %s (%s)", err->message,
        dbg ? dbg : "no debug");
    g_clear_error (&err);
    g_free (dbg);
    gst_message_unref (msg);
  }
}

/*
 * The consumer reaches PLAYING while the producer has not yet delivered caps,
 * so nothing primes the listener before the base source's create loop runs its
 * first negotiation. That is the window the deferral closes: without it the
 * unfixated videocrop range makes basesrc give up with not-negotiated, and the
 * leg stays dead even after the producer's fixed caps show up.
 */
GST_START_TEST (interpipe_videocrop_negotiates_when_producer_caps_arrive_late)
{
  GstPipeline *producer;
  GstPipeline *consumer;

  consumer = create_consumer ("vcnode");

  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (consumer), GST_STATE_PLAYING));

  /* Let the create loop reach its first negotiation with the node still absent
   * and the listener still unprimed. */
  g_usleep (200 * G_TIME_SPAN_MILLISECOND);

  producer = create_producer ("vcnode");
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (producer), GST_STATE_PLAYING));

  assert_producer_caps_reached_appsink (consumer);
  assert_no_error_on_bus (consumer);

  gst_element_set_state (GST_ELEMENT (consumer), GST_STATE_NULL);
  gst_element_set_state (GST_ELEMENT (producer), GST_STATE_NULL);
  gst_object_unref (consumer);
  gst_object_unref (producer);
}

GST_END_TEST;

/*
 * The same leg attaching to a producer that is already running. Here the
 * producer's caps are on the node by the time the listener registers, so the
 * listener is primed during start and negotiation has fixed caps to work with.
 * Guards the deferral against regressing the ordinary attach.
 */
GST_START_TEST (interpipe_videocrop_negotiates_on_attach_to_running_producer)
{
  GstPipeline *producer;
  GstPipeline *consumer;

  producer = create_producer ("vcnode_running");
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (producer), GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_get_state (GST_ELEMENT (producer), NULL, NULL,
          GST_CLOCK_TIME_NONE));

  /* Let the producer actually push, so the node carries caps on attach. */
  g_usleep (200 * G_TIME_SPAN_MILLISECOND);

  consumer = create_consumer ("vcnode_running");
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (consumer), GST_STATE_PLAYING));

  assert_producer_caps_reached_appsink (consumer);
  assert_no_error_on_bus (consumer);

  gst_element_set_state (GST_ELEMENT (consumer), GST_STATE_NULL);
  gst_element_set_state (GST_ELEMENT (producer), GST_STATE_NULL);
  gst_object_unref (consumer);
  gst_object_unref (producer);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("videocrop_format");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, interpipe_videocrop_negotiates_when_producer_caps_arrive_late);
  tcase_add_test (tc, interpipe_videocrop_negotiates_on_attach_to_running_producer);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
