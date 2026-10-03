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

/*
 * A flushing seek on the producer is forwarded to every listener, and the
 * consumer has to come out of it streaming again, whatever its streaming
 * thread was doing when the flush arrived.
 *
 * The case that matters is a flush landing while the consumer's streaming
 * thread is inside the push of a buffer: the push returns FLUSHING and the
 * base class pauses its streaming task. Only the base class's own flush
 * handling restarts that task, so a FLUSH_STOP that never goes through it
 * leaves the consumer stopped for good.
 *
 * The race is made deterministic with a probe on the consumer's source pad
 * that holds one buffer inside its push until the forwarded FLUSH_START has
 * reached the consumer's sink. The push then fails with FLUSHING every time.
 * After the seek the consumer must deliver buffers again, each under a
 * segment: its own (starting at 0) for restart-ts, which re-stamps buffers,
 * and the producer's (starting at the seek position) for passthrough-ts and
 * compensate-ts, which keep the producer's timeline.
 */

#define SEEK_POSITION (10 * GST_SECOND)
#define BUFFERS_WANTED 10
#define WAIT_TIMEOUT (5 * G_TIME_SPAN_SECOND)

typedef struct
{
  GMutex lock;
  GCond cond;
  /* Hold the next buffer pushed by the consumer's source until flush_seen. */
  gboolean arm;
  gboolean holding;
  /* The forwarded FLUSH_START / FLUSH_STOP reached the consumer's sink. */
  gboolean flush_seen;
  gboolean flush_stopped;
  guint buffers_before;
  /* What reached the consumer's sink after the FLUSH_STOP. */
  guint buffers_after;
  guint unsegmented;
  guint wrong_segment;
  guint64 segment_start;
  guint64 expected_start;
} Race;

static GstPadProbeReturn
hold_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Race *r = user_data;
  gint64 deadline;

  g_mutex_lock (&r->lock);
  if (r->arm) {
    r->arm = FALSE;
    r->holding = TRUE;
    g_cond_broadcast (&r->cond);
    deadline = g_get_monotonic_time () + WAIT_TIMEOUT;
    while (!r->flush_seen)
      if (!g_cond_wait_until (&r->cond, &r->lock, deadline))
        break;
  }
  g_mutex_unlock (&r->lock);

  return GST_PAD_PROBE_OK;
}

static GstPadProbeReturn
sink_probe (GstPad * pad, GstPadProbeInfo * info, gpointer user_data)
{
  Race *r = user_data;

  g_mutex_lock (&r->lock);
  if (GST_PAD_PROBE_INFO_TYPE (info) & GST_PAD_PROBE_TYPE_BUFFER) {
    if (r->flush_stopped) {
      GstEvent *sticky = gst_pad_get_sticky_event (pad, GST_EVENT_SEGMENT, 0);

      r->buffers_after++;
      if (!sticky)
        r->unsegmented++;
      else
        gst_event_unref (sticky);
      if (r->segment_start != r->expected_start)
        r->wrong_segment++;
    } else {
      r->buffers_before++;
    }
  } else {
    GstEvent *event = GST_PAD_PROBE_INFO_EVENT (info);

    switch (GST_EVENT_TYPE (event)) {
      case GST_EVENT_FLUSH_START:
        r->flush_seen = TRUE;
        break;
      case GST_EVENT_FLUSH_STOP:
        r->flush_stopped = TRUE;
        r->segment_start = G_MAXUINT64;
        break;
      case GST_EVENT_SEGMENT:
      {
        const GstSegment *segment;

        gst_event_parse_segment (event, &segment);
        r->segment_start = segment->start;
        break;
      }
      default:
        break;
    }
  }
  g_cond_broadcast (&r->cond);
  g_mutex_unlock (&r->lock);

  return GST_PAD_PROBE_OK;
}

static void
add_probe (GstElement * pipeline, const gchar * element, const gchar * pad,
    GstPadProbeType mask, GstPadProbeCallback callback, Race * r)
{
  GstElement *e;
  GstPad *p;

  e = gst_bin_get_by_name (GST_BIN (pipeline), element);
  fail_unless (e != NULL);
  p = gst_element_get_static_pad (e, pad);
  fail_unless (p != NULL);
  gst_pad_add_probe (p, mask, callback, r, NULL);
  gst_object_unref (p);
  gst_object_unref (e);
}

/* Wait under r->lock until *counter reaches wanted or the timeout passes. */
static void
wait_count (Race * r, guint * counter, guint wanted)
{
  gint64 deadline = g_get_monotonic_time () + WAIT_TIMEOUT;

  while (*counter < wanted)
    if (!g_cond_wait_until (&r->cond, &r->lock, deadline))
      break;
}

static void
flush_while_pushing (const gchar * stream_sync, guint64 expected_start)
{
  GstElement *producer, *consumer;
  gchar *desc;
  gint64 deadline;
  Race r = { 0, };

  g_mutex_init (&r.lock);
  g_cond_init (&r.cond);
  r.expected_start = expected_start;

  producer =
      gst_parse_launch
      ("videotestsrc ! video/x-raw,width=64,height=48,framerate=30/1 "
      "! interpipesink name=racenode sync=true", NULL);
  fail_if (producer == NULL);

  desc = g_strdup_printf ("interpipesrc name=src listen-to=racenode "
      "is-live=true format=time stream-sync=%s ! fakesink name=fsink "
      "sync=false async=false", stream_sync);
  consumer = gst_parse_launch (desc, NULL);
  g_free (desc);
  fail_if (consumer == NULL);

  add_probe (consumer, "src", "src", GST_PAD_PROBE_TYPE_BUFFER, hold_probe,
      &r);
  add_probe (consumer, "fsink", "sink", GST_PAD_PROBE_TYPE_BUFFER |
      GST_PAD_PROBE_TYPE_EVENT_DOWNSTREAM | GST_PAD_PROBE_TYPE_EVENT_FLUSH,
      sink_probe, &r);

  fail_if (gst_element_set_state (consumer, GST_STATE_PLAYING) ==
      GST_STATE_CHANGE_FAILURE);
  fail_if (gst_element_set_state (producer, GST_STATE_PLAYING) ==
      GST_STATE_CHANGE_FAILURE);
  fail_if (gst_element_get_state (producer, NULL, NULL,
          GST_CLOCK_TIME_NONE) == GST_STATE_CHANGE_FAILURE);

  /* Streaming, then one buffer held inside the consumer's push. */
  g_mutex_lock (&r.lock);
  wait_count (&r, &r.buffers_before, 5);
  fail_unless (r.buffers_before >= 5, "only %u buffers before the seek",
      r.buffers_before);
  r.arm = TRUE;
  deadline = g_get_monotonic_time () + WAIT_TIMEOUT;
  while (!r.holding)
    if (!g_cond_wait_until (&r.cond, &r.lock, deadline))
      break;
  fail_unless (r.holding, "the consumer never pushed a buffer to hold");
  g_mutex_unlock (&r.lock);

  fail_unless (gst_element_seek_simple (producer, GST_FORMAT_TIME,
          GST_SEEK_FLAG_FLUSH, SEEK_POSITION));

  g_mutex_lock (&r.lock);
  fail_unless (r.flush_seen, "the flush never reached the consumer");
  wait_count (&r, &r.buffers_after, BUFFERS_WANTED);
  g_mutex_unlock (&r.lock);

  fail_if (gst_element_set_state (consumer, GST_STATE_NULL) ==
      GST_STATE_CHANGE_FAILURE);
  fail_if (gst_element_set_state (producer, GST_STATE_NULL) ==
      GST_STATE_CHANGE_FAILURE);

  fail_unless (r.flush_stopped, "the flush never stopped on the consumer");
  fail_unless (r.buffers_after >= BUFFERS_WANTED,
      "only %u buffers reached the consumer's sink after the flush",
      r.buffers_after);
  fail_unless_equals_int (r.unsegmented, 0);
  fail_unless_equals_int (r.wrong_segment, 0);

  gst_object_unref (consumer);
  gst_object_unref (producer);
  g_cond_clear (&r.cond);
  g_mutex_clear (&r.lock);
}

GST_START_TEST (test_flush_while_pushing_restart_ts)
{
  flush_while_pushing ("restart-ts", 0);
}

GST_END_TEST;

GST_START_TEST (test_flush_while_pushing_passthrough_ts)
{
  flush_while_pushing ("passthrough-ts", SEEK_POSITION);
}

GST_END_TEST;

GST_START_TEST (test_flush_while_pushing_compensate_ts)
{
  flush_while_pushing ("compensate-ts", SEEK_POSITION);
}

GST_END_TEST;

static Suite *
gst_interpipe_suite (void)
{
  Suite *suite = suite_create ("interpipe_flush_forwarding");
  TCase *tc = tcase_create ("general");

  suite_add_tcase (suite, tc);
  tcase_add_test (tc, test_flush_while_pushing_restart_ts);
  tcase_add_test (tc, test_flush_while_pushing_passthrough_ts);
  tcase_add_test (tc, test_flush_while_pushing_compensate_ts);

  return suite;
}

GST_CHECK_MAIN (gst_interpipe);
