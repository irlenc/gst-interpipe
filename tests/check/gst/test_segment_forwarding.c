/* GStreamer
 * Copyright (C) 2026 RidgeRun
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
#include <gst/app/gstappsink.h>

/*
 * The publisher's segment must not reach the consumer's downstream.
 *
 * interpipesrc is an appsrc subclass and pushes its own TIME segment, which is
 * the one that describes what leaves its source pad: both stream-sync modes put
 * buffers on this element's timeline, restart-ts by rebasing them and
 * compensate-ts by shifting them by the base-time difference. Forwarding the
 * publisher's segment as well puts a second segment downstream, and downstream
 * keeps the last one it saw. When the two timelines disagree, every buffer this
 * element produces then falls outside the active segment and the next sink
 * clips the entire stream away, silently: no error, no warning, no bus message,
 * and the element's own logs show every buffer arriving and being dequeued.
 *
 * It was found with a VA decoder publishing into a hop, where 602 buffers were
 * published, 602 were dequeued by the consumer, and 601 were dropped by the
 * consumer's sink as "out of clipping segment". It stayed hidden for as long as
 * it did because a publisher whose segment starts at zero, which is every
 * publisher in this suite and every videotestsrc, forwards a segment that
 * happens to agree with the rebased buffers.
 *
 * So the invariant is asserted structurally rather than by reproducing one
 * publisher's timeline: downstream of an interpipesrc there is exactly one
 * segment event, and buffers keep flowing.
 */

typedef struct
{
  guint segments;
  guint buffers;
} Counts;

/* What reached the consumer's downstream after the producer's flushing seek:
 * segments, their starts, and buffers pushed while no segment was in force on
 * the pad. */
typedef struct
{
  GMutex lock;
  gboolean seeked;
  guint segments;
  guint nonzero_starts;
  guint buffers;
  guint unsegmented;
} FlushCounts;

static GstPadProbeReturn
count_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Counts *counts = user_data;

  if (GST_PAD_PROBE_INFO_TYPE (info) & GST_PAD_PROBE_TYPE_BUFFER) {
    counts->buffers++;
  } else if (GST_PAD_PROBE_INFO_TYPE (info) &
      GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM) {
    GstEvent *event = GST_PAD_PROBE_INFO_EVENT (info);
    if (GST_EVENT_TYPE (event) == GST_EVENT_SEGMENT)
      counts->segments++;
  }

  return GST_PAD_PROBE_OK;
}

GST_START_TEST (test_one_segment_downstream)
{
  GstElement *producer, *consumer;
  GstElement *interpipesrc;
  GstPad *srcpad;
  Counts counts = { 0, 0 };
  guint waited;

  producer =
      gst_parse_launch
      ("videotestsrc is-live=true ! video/x-raw,width=64,height=48,framerate=30/1 "
      "! interpipesink name=segmentnode sync=false async=false", NULL);
  fail_if (producer == NULL);

  consumer =
      gst_parse_launch
      ("interpipesrc name=src listen-to=segmentnode is-live=true format=time "
      "stream-sync=restart-ts ! appsink name=probe sync=false", NULL);
  fail_if (consumer == NULL);

  interpipesrc = gst_bin_get_by_name (GST_BIN (consumer), "src");
  fail_if (interpipesrc == NULL);
  srcpad = gst_element_get_static_pad (interpipesrc, "src");
  fail_if (srcpad == NULL);
  gst_pad_add_probe (srcpad,
      GST_PAD_PROBE_TYPE_BUFFER | GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM,
      count_probe, &counts, NULL);

  gst_element_set_state (producer, GST_STATE_PLAYING);
  gst_element_set_state (consumer, GST_STATE_PLAYING);

  /* Long enough for several buffers at 30fps, however the host is loaded. */
  for (waited = 0; waited < 100 && counts.buffers < 10; waited++)
    g_usleep (50 * G_TIME_SPAN_MILLISECOND);

  gst_element_set_state (consumer, GST_STATE_NULL);
  gst_element_set_state (producer, GST_STATE_NULL);

  fail_unless (counts.buffers >= 10,
      "only %u buffers left interpipesrc", counts.buffers);
  fail_unless_equals_int (counts.segments, 1);

  gst_object_unref (srcpad);
  gst_object_unref (interpipesrc);
  gst_object_unref (consumer);
  gst_object_unref (producer);
}

GST_END_TEST;

static GstPadProbeReturn
flush_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  FlushCounts *counts = user_data;
  GstEvent *sticky;

  g_mutex_lock (&counts->lock);
  if (!counts->seeked) {
    g_mutex_unlock (&counts->lock);
    return GST_PAD_PROBE_OK;
  }

  if (GST_PAD_PROBE_INFO_TYPE (info) & GST_PAD_PROBE_TYPE_BUFFER) {
    counts->buffers++;
    sticky = gst_pad_get_sticky_event (pad, GST_EVENT_SEGMENT, 0);
    if (sticky)
      gst_event_unref (sticky);
    else
      counts->unsegmented++;
  } else if (GST_EVENT_TYPE (GST_PAD_PROBE_INFO_EVENT (info)) ==
      GST_EVENT_SEGMENT) {
    const GstSegment *segment;

    gst_event_parse_segment (GST_PAD_PROBE_INFO_EVENT (info), &segment);
    counts->segments++;
    if (segment->start != 0)
      counts->nonzero_starts++;
  }
  g_mutex_unlock (&counts->lock);

  return GST_PAD_PROBE_OK;
}

/*
 * A flushing seek on the producer is forwarded through the node, and the
 * FLUSH_STOP that reaches this element's downstream clears the segment in
 * force there. A restart-ts consumer drops the producer's segment that
 * follows, so a segment of its own timeline has to be restored before the
 * next buffer. Without it every buffer after the seek flows with no segment
 * at all. test_flush_forwarding covers a flush landing mid-push.
 */
GST_START_TEST (test_segment_restored_after_producer_flush)
{
  GstElement *producer, *consumer;
  GstElement *interpipesrc;
  GstPad *srcpad;
  FlushCounts counts = { {0}, FALSE, 0, 0, 0, 0 };
  guint waited, buffers;

  g_mutex_init (&counts.lock);

  producer =
      gst_parse_launch
      ("videotestsrc ! video/x-raw,width=64,height=48,framerate=30/1 "
      "! interpipesink name=flushnode sync=true", NULL);
  fail_if (producer == NULL);

  consumer =
      gst_parse_launch
      ("interpipesrc name=src listen-to=flushnode is-live=true format=time "
      "stream-sync=restart-ts ! fakesink sync=false async=false", NULL);
  fail_if (consumer == NULL);

  interpipesrc = gst_bin_get_by_name (GST_BIN (consumer), "src");
  fail_if (interpipesrc == NULL);
  srcpad = gst_element_get_static_pad (interpipesrc, "src");
  fail_if (srcpad == NULL);
  gst_pad_add_probe (srcpad,
      GST_PAD_PROBE_TYPE_BUFFER | GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM,
      flush_probe, &counts, NULL);

  fail_if (gst_element_set_state (consumer, GST_STATE_PLAYING) ==
      GST_STATE_CHANGE_FAILURE);
  fail_if (gst_element_set_state (producer, GST_STATE_PLAYING) ==
      GST_STATE_CHANGE_FAILURE);
  fail_if (gst_element_get_state (producer, NULL, NULL,
          GST_CLOCK_TIME_NONE) == GST_STATE_CHANGE_FAILURE);

  /* Let the stream settle before the seek. */
  g_usleep (300 * G_TIME_SPAN_MILLISECOND);

  g_mutex_lock (&counts.lock);
  counts.seeked = TRUE;
  g_mutex_unlock (&counts.lock);
  fail_unless (gst_element_seek_simple (producer, GST_FORMAT_TIME,
          GST_SEEK_FLAG_FLUSH, 10 * GST_SECOND));

  for (waited = 0; waited < 100; waited++) {
    g_mutex_lock (&counts.lock);
    buffers = counts.buffers;
    g_mutex_unlock (&counts.lock);
    if (buffers >= 10)
      break;
    g_usleep (50 * G_TIME_SPAN_MILLISECOND);
  }

  gst_element_set_state (consumer, GST_STATE_NULL);
  gst_element_set_state (producer, GST_STATE_NULL);

  fail_unless (counts.buffers >= 10,
      "only %u buffers left interpipesrc after the seek", counts.buffers);
  fail_unless (counts.segments >= 1, "no segment restored after the flush");
  fail_unless_equals_int (counts.nonzero_starts, 0);
  fail_unless_equals_int (counts.unsegmented, 0);

  g_mutex_clear (&counts.lock);
  gst_object_unref (srcpad);
  gst_object_unref (interpipesrc);
  gst_object_unref (consumer);
  gst_object_unref (producer);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("interpipe_segment_forwarding");
  TCase *tc = tcase_create ("general");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, test_one_segment_downstream);
  tcase_add_test (tc, test_segment_restored_after_producer_flush);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
