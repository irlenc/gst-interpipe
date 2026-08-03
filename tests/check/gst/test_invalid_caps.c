/* GStreamer
 * Copyright (C) 2016 Carlos Rodriguez <carlos.rodriguez@ridgerun.com>
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
#include <gst/video/gstvideometa.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>

/*
 * Given one interpipesrc and one interpipesink with no caps intersection, the
 * caps must not be set, and the sink must say so on its bus.
 *
 * The no-intersection path detaches every listener, and a detached listener
 * leaves the registry's listener table, so it is never notified when the node
 * republishes: the disconnection is permanent. This used to be reported only as
 * a GST_ERROR log line while the sink stayed happily in PLAYING, which is a
 * producer that looks alive and serves nobody, forever. The sink now posts an
 * element error, so a supervising application finds out.
 */

GST_START_TEST (invalid_caps)
{
  GstPipeline *sink;
  GstPipeline *src;
  GstElement *intersink;
  GstElement *intersrc;
  GstCaps *caps1;
  GstCaps *caps2;
  GstMessage *msg;
  GstBus *bus;
  GError *error = NULL;

  /* Create two sink pipelines */
  sink =
      GST_PIPELINE (gst_parse_launch
      ("videotestsrc name=vtsrc ! capsfilter caps=video/x-raw,width=640,height=480,framerate=(fraction)30/1 ! interpipesink "
          "name=intersink sync=true", &error));
  fail_if (error);
  intersink = gst_bin_get_by_name (GST_BIN (sink), "intersink");

  /* Create one source pipeline */
  src =
      GST_PIPELINE (gst_parse_launch
      ("interpipesrc name=intersrc listen-to=intersink ! capsfilter caps=video/x-raw,format=(string)I420,width=320,height=240,framerate=(fraction)30/1 ! "
          "appsink name=asink async=false", &error));
  fail_if (error);
  intersrc = gst_bin_get_by_name (GST_BIN (src), "intersrc");

  /* 
   * Play the pipelines
   * gst_element_get_state blocks up execution until the state change is
   * completed. It's used here to guarantee a secuential pipeline initialization
   * and avoid concurrency errors.
   */
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (GST_ELEMENT (src),
          GST_STATE_PLAYING));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_get_state (GST_ELEMENT (src),
          NULL, NULL, GST_CLOCK_TIME_NONE));
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_PLAYING));
  /* No get_state on the sink here: it is expected to fail its state change now
   * that the empty intersection is an error, and that is what the bus check
   * below asserts. */

  /* The sink reports the failed negotiation on its bus rather than detaching
   * every listener quietly. */
  bus = gst_pipeline_get_bus (sink);
  msg = gst_bus_timed_pop_filtered (bus, 5 * GST_SECOND, GST_MESSAGE_ERROR);
  fail_if (!msg, "the sink must post an error when no listener caps intersect");
  gst_message_unref (msg);
  gst_object_unref (bus);

  /* The listener never gets caps: that is the no-intersection case itself.
   *
   * Whether the sink has caps by now is deliberately not asserted. It races its
   * own error: the upstream caps may or may not have been set on the appsink
   * before the failed intersection stopped the state change, and both outcomes
   * are correct. The error above is the contract; this is timing. */
  caps1 = gst_app_src_get_caps (GST_APP_SRC (intersrc));
  fail_if (caps1);

  caps2 = gst_app_sink_get_caps (GST_APP_SINK (intersink));
  if (caps2)
    gst_caps_unref (caps2);

  /* Stop pipelines */
  fail_if (GST_STATE_CHANGE_FAILURE ==
      gst_element_set_state (GST_ELEMENT (sink), GST_STATE_NULL));
  fail_if (GST_STATE_CHANGE_FAILURE == gst_element_set_state (GST_ELEMENT (src),
          GST_STATE_NULL));

  /* Cleanup */
  g_object_unref (intersink);
  g_object_unref (intersrc);
  g_object_unref (sink);
  g_object_unref (src);
}

GST_END_TEST;


static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("Interpipe");
  TCase *tc1 = tcase_create ("invalid_caps");

  suite_add_tcase (suite, tc1);
  tcase_add_test (tc1, invalid_caps);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
