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
 * An interpipesink refuses upstream events while more than one listener is
 * attached, so that one consumer cannot disturb the others. A force-key-unit
 * request is the exception: the producer answers it by emitting one extra
 * keyframe, which every listener receives. Without that exception a consumer
 * sharing a node can never ask for a keyframe and stays blank until the next
 * periodic one.
 */

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <gst/check/gstcheck.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

/* Set from the producer's event handler when the matching event lands. */
static gboolean force_key_unit_arrived;
static gboolean other_event_arrived;

#define OTHER_EVENT_NAME "GstInterPipeTestEvent"

/* Built the same way gst_video_event_new_upstream_force_key_unit does, so the
 * test does not need to link gstreamer-video. */
static GstEvent *
new_force_key_unit_event (void)
{
  return gst_event_new_custom (GST_EVENT_CUSTOM_UPSTREAM,
      gst_structure_new ("GstForceKeyUnit",
          "all-headers", G_TYPE_BOOLEAN, TRUE, NULL));
}

/* A custom upstream event that is not a force-key-unit. It must stay confined
 * to the single-listener case. */
static GstEvent *
new_other_upstream_event (void)
{
  return gst_event_new_custom (GST_EVENT_CUSTOM_UPSTREAM,
      gst_structure_new_empty (OTHER_EVENT_NAME));
}

static gboolean
producer_event_handler (GstPad * pad, GstObject * parent, GstEvent * event)
{
  const GstStructure *structure = gst_event_get_structure (event);

  if (GST_EVENT_TYPE (event) == GST_EVENT_CUSTOM_UPSTREAM && structure) {
    if (gst_structure_has_name (structure, "GstForceKeyUnit"))
      force_key_unit_arrived = TRUE;
    else if (gst_structure_has_name (structure, OTHER_EVENT_NAME))
      other_event_arrived = TRUE;
  }

  return gst_pad_event_default (pad, parent, event);
}

/* One producer node with two listeners hanging off it. Returns the pad the
 * test pushes upstream events into (a consumer sink pad), and the producer pad
 * that is watched for the forwarded event. */
typedef struct
{
  GstElement *producer;
  GstElement *consumer1;
  GstElement *consumer2;
  GstPad *producer_srcpad;
  GstPad *consumer_sinkpad;
} TwoListenerFixture;

static void
two_listener_fixture_setup (TwoListenerFixture * f, const gchar * node)
{
  GstElement *appsrc, *intersink, *intersrc, *fsink, *intersrc2, *fsink2;
  gchar *desc;
  GError *error = NULL;

  force_key_unit_arrived = FALSE;
  other_event_arrived = FALSE;

  f->producer = gst_pipeline_new ("producer");
  f->consumer1 = gst_pipeline_new ("consumer1");
  f->consumer2 = gst_pipeline_new ("consumer2");

  desc = g_strdup_printf ("interpipesink name=%s sync=true", node);
  intersink = gst_parse_launch (desc, &error);
  g_free (desc);
  fail_if (error);

  appsrc = gst_parse_launch ("appsrc name=appsrc", &error);
  fail_if (error);

  desc = g_strdup_printf ("interpipesrc name=c1 listen-to=%s", node);
  intersrc = gst_parse_launch (desc, &error);
  g_free (desc);
  fail_if (error);

  fsink = gst_parse_launch ("fakesink sync=true async=false", &error);
  fail_if (error);

  desc = g_strdup_printf ("interpipesrc name=c2 listen-to=%s", node);
  intersrc2 = gst_parse_launch (desc, &error);
  g_free (desc);
  fail_if (error);

  fsink2 = gst_parse_launch ("fakesink sync=true async=false", &error);
  fail_if (error);

  gst_bin_add_many (GST_BIN (f->producer), appsrc, intersink, NULL);
  gst_element_link_many (appsrc, intersink, NULL);
  gst_bin_add_many (GST_BIN (f->consumer1), intersrc, fsink, NULL);
  gst_element_link_many (intersrc, fsink, NULL);
  gst_bin_add_many (GST_BIN (f->consumer2), intersrc2, fsink2, NULL);
  gst_element_link_many (intersrc2, fsink2, NULL);

  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (f->producer, GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (f->consumer1, GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (f->consumer2, GST_STATE_PLAYING));

  f->producer_srcpad = gst_element_get_static_pad (appsrc, "src");
  fail_if (!f->producer_srcpad);
  f->consumer_sinkpad = gst_element_get_static_pad (fsink, "sink");
  fail_if (!f->consumer_sinkpad);

  /* Watch what actually reaches the producer, upstream of the interpipesink. */
  gst_pad_set_event_function (f->producer_srcpad, producer_event_handler);
}

static void
two_listener_fixture_teardown (TwoListenerFixture * f)
{
  gst_element_set_state (f->producer, GST_STATE_NULL);
  gst_element_set_state (f->consumer1, GST_STATE_NULL);
  gst_element_set_state (f->consumer2, GST_STATE_NULL);

  gst_object_unref (f->producer_srcpad);
  gst_object_unref (f->consumer_sinkpad);
  gst_object_unref (f->producer);
  gst_object_unref (f->consumer1);
  gst_object_unref (f->consumer2);
}

/*
 * Two listeners share the node. The force-key-unit must still reach the
 * producer, where a generic upstream event would be refused.
 */
GST_START_TEST (interpipe_force_key_unit_reaches_producer_with_two_listeners)
{
  TwoListenerFixture f;

  two_listener_fixture_setup (&f, "fku_node");

  /* The return value of the push is not a reliable signal: GstBaseSrc answers
   * FALSE for custom upstream events it does not recognise. What matters is
   * whether the event reached the shared producer. */
  gst_pad_push_event (f.consumer_sinkpad, new_force_key_unit_event ());

  fail_unless (force_key_unit_arrived,
      "force-key-unit was not forwarded to the producer with two listeners");

  two_listener_fixture_teardown (&f);
}

GST_END_TEST;

/*
 * The negative control. Only the force-key-unit is exempt: any other upstream
 * event stays refused while several listeners share the node, so one consumer
 * still cannot renegotiate or seek on behalf of the others.
 */
GST_START_TEST (interpipe_other_upstream_event_refused_with_two_listeners)
{
  TwoListenerFixture f;

  two_listener_fixture_setup (&f, "other_node");

  gst_pad_push_event (f.consumer_sinkpad, new_other_upstream_event ());

  fail_if (other_event_arrived,
      "a non force-key-unit upstream event reached the producer with two "
      "listeners, so the exemption is too broad");

  two_listener_fixture_teardown (&f);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc = tcase_create ("force_key_unit_event");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc,
      interpipe_force_key_unit_reaches_producer_with_two_listeners);
  tcase_add_test (tc, interpipe_other_upstream_event_refused_with_two_listeners);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
