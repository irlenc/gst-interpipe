/* GStreamer
 * Copyright (C) 2013-2016 Michael Grüner <michael.gruner@ridgerun.com>
 * Copyright (C) 2016 Carlos Rodriguez <carlos.rodriguez@ridgerun.com>
 * Copyright (C) 2016 Erick Arroyo <erick.arroyo@ridgerun.com>
 * Copyright (C) 2016 Marco Madrigal <marco.madrigal@ridgerun.com>
 *
 * This file is part of gst-interpipe-1.0
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

#include <gst/gst.h>
#include <string.h>

#include "gstinterpipe.h"

/**
 * SECTION:gstinterpipe
 *
 * GstInterpipe Core handling inter pipeline communication. 
 */

GST_DEBUG_CATEGORY (gst_inter_pipe_debug);
#define GST_CAT_DEFAULT gst_inter_pipe_debug

typedef struct _GstInterPipeListenerPriv GstInterPipeListenerPriv;
struct _GstInterPipeListenerPriv
{
  /* Strong reference. The table owns a ref on each registered listener so a
   * listener finalized without leaving the table cannot be freed while an
   * add_node/remove_node notification is dispatching to it. */
  GstInterPipeIListener *listener;
  /* Owned copy of the node name this listener is attached to. Duplicated on
   * store and freed on replace/remove so it never depends on the listener's
   * own listen-to string staying alive (which a concurrent property set could
   * free). */
  gchar *listen_to;
};

/* Global mutexes for singletons */
static GRecMutex listeners_mutex;
static GMutex nodes_mutex;

static GHashTable *gst_inter_pipe_get_listeners ();
static GHashTable *gst_inter_pipe_get_nodes ();
static void gst_inter_pipe_notify_node_added (gpointer _listener_key,
    gpointer _listener, gpointer data);
static gboolean gst_inter_pipe_leave_listeners_table (GstInterPipeIListener *
    listener);
static gboolean gst_inter_pipe_leave_node_priv (GstInterPipeIListener *
    listener);
static void gst_inter_pipe_listener_priv_free (gpointer data);

static void
gst_inter_pipe_listener_priv_free (gpointer data)
{
  GstInterPipeListenerPriv *listener_priv = data;

  gst_object_unref (listener_priv->listener);
  g_free (listener_priv->listen_to);
  g_free (listener_priv);
}

static GHashTable *
gst_inter_pipe_get_listeners (void)
{
  /* The listeners singleton */
  static GHashTable *gst_inter_pipe_listeners = NULL;

  if (!gst_inter_pipe_listeners) {
    /* Key by the listener object: its pointer is stable for its lifetime and
     * unique across pipelines, whereas its name is borrowed memory that a
     * rename frees and that two listeners in different pipelines can share. The
     * value owns a ref on the listener, so an entry can never outlive the
     * object it points at. */
    gst_inter_pipe_listeners =
        g_hash_table_new_full (g_direct_hash, g_direct_equal, NULL,
        gst_inter_pipe_listener_priv_free);
  }
  return gst_inter_pipe_listeners;
}

/* Value destroy for the nodes table: clear and free the GWeakRef box. */
static void
gst_inter_pipe_node_weak_ref_free (gpointer data)
{
  GWeakRef *weak = (GWeakRef *) data;

  g_weak_ref_clear (weak);
  g_free (weak);
}

static GHashTable *
gst_inter_pipe_get_nodes (void)
{
  /* The nodes singleton */
  static GHashTable *gst_inter_pipe_nodes = NULL;

  if (!gst_inter_pipe_nodes) {
    /* Own the key strings so the table never depends on the node's name
     * outliving its entry. Values are GWeakRef boxes rather than borrowed
     * strong pointers, so a lookup can never resurrect a finalizing node. See
     * gst_inter_pipe_get_node. */
    gst_inter_pipe_nodes =
        g_hash_table_new_full (g_str_hash, g_str_equal, g_free,
        gst_inter_pipe_node_weak_ref_free);
  }
  return gst_inter_pipe_nodes;
}

GstInterPipeINode *
gst_inter_pipe_get_node (const gchar * node_name)
{
  GHashTable *nodes;
  GWeakRef *weak;
  GstInterPipeINode *value = NULL;

  g_return_val_if_fail (node_name != NULL, NULL);

  g_mutex_lock (&nodes_mutex);
  nodes = gst_inter_pipe_get_nodes ();

  /* g_weak_ref_get atomically returns a new strong reference, or NULL once the
   * node has dropped to refcount 0 and is being finalized. Taking gst_object_ref
   * on a borrowed pointer instead would resurrect a node that another thread is
   * concurrently tearing down (refcount 0 -> 1 -> 0 = double finalize), leaving
   * that thread's set_state and bin_remove operating on freed memory. */
  weak = (GWeakRef *) g_hash_table_lookup (nodes, node_name);
  if (weak)
    value = (GstInterPipeINode *) g_weak_ref_get (weak);
  g_mutex_unlock (&nodes_mutex);

  return value;
}

gboolean
gst_inter_pipe_listen_node (GstInterPipeIListener * listener,
    const gchar * node_name)
{
  GstInterPipeINode *node;
  GstInterPipeListenerPriv *listener_priv;
  GHashTable *listeners;
  const gchar *listener_name;
  gboolean priv_is_new = FALSE;

  g_return_val_if_fail (listener != NULL, FALSE);
  g_return_val_if_fail (node_name != NULL, FALSE);

  g_rec_mutex_lock (&listeners_mutex);

  listeners = gst_inter_pipe_get_listeners ();
  listener_name = gst_inter_pipe_ilistener_get_name (listener);

  GST_INFO ("listener %s listen to node %s", listener_name, node_name);
  listener_priv =
      (GstInterPipeListenerPriv *) g_hash_table_lookup (listeners, listener);
  if (listener_priv) {
    if (!g_strcmp0 (listener_priv->listen_to, node_name))
      goto already_listen;

    if (listener_priv->listen_to)
      gst_inter_pipe_leave_node_priv (listener);

  } else {
    listener_priv = g_malloc (sizeof (GstInterPipeListenerPriv));
    listener_priv->listener = gst_object_ref (listener);
    listener_priv->listen_to = NULL;
    priv_is_new = TRUE;
  }

  GST_INFO ("Adding new listener %s to node %s", listener_name, node_name);

  node = gst_inter_pipe_get_node (node_name);

  /* If the node is not in the list we will notify later
     when it connects */
  if (node == NULL) {
    GST_INFO ("Node is not available yet, connecting later.");
    g_free (listener_priv->listen_to);
    listener_priv->listen_to = NULL;
  } else {
    if (!gst_inter_pipe_inode_add_listener (node, listener))
      goto add_failed;
    g_free (listener_priv->listen_to);
    listener_priv->listen_to = g_strdup (node_name);
    gst_object_unref (node);
  }

  /* An existing priv is already in the table and was updated in place.
   * Re-inserting it would destroy the value being inserted. */
  if (priv_is_new)
    g_hash_table_insert (listeners, (gpointer) listener,
        (gpointer) listener_priv);

  g_rec_mutex_unlock (&listeners_mutex);

  return TRUE;
already_listen:
  {
    GST_INFO ("Already listening to node %s", node_name);
    g_rec_mutex_unlock (&listeners_mutex);
    return TRUE;
  }
add_failed:
  {
    GST_WARNING ("Could not add listener %s to node %s", listener_name,
        node_name);
    /* We reach here only from the branch that holds a node reference. */
    gst_object_unref (node);
    /* A freshly allocated priv was never inserted into the table, so free it
     * here (dropping the ref it took). An existing priv is owned by the table
     * and left in place. */
    if (priv_is_new)
      gst_inter_pipe_listener_priv_free (listener_priv);
    g_rec_mutex_unlock (&listeners_mutex);
    return FALSE;
  }
}

static gboolean
gst_inter_pipe_leave_listeners_table (GstInterPipeIListener * listener)
{
  GHashTable *listeners;

  listeners = gst_inter_pipe_get_listeners ();

  /* Removing the entry runs the value destroy notify, which frees the priv and
   * drops the table's ref on the listener. */
  return g_hash_table_remove (listeners, listener);
}

static gboolean
gst_inter_pipe_leave_node_priv (GstInterPipeIListener * listener)
{
  GHashTable *listeners;
  GstInterPipeINode *node;
  GstInterPipeListenerPriv *listener_priv;
  const gchar *listener_name;

  g_return_val_if_fail (listener != NULL, FALSE);

  listeners = gst_inter_pipe_get_listeners ();
  listener_name = gst_inter_pipe_ilistener_get_name (listener);

  listener_priv =
      (GstInterPipeListenerPriv *) g_hash_table_lookup (listeners, listener);
  if (!listener_priv)
    goto no_listener;

  if (listener_priv->listen_to) {
    GST_INFO ("listener %s leaving node %s", listener_name,
        listener_priv->listen_to);

    node = gst_inter_pipe_get_node (listener_priv->listen_to);
    if (node == NULL)
      goto no_node;

    if (!gst_inter_pipe_inode_remove_listener (node, listener))
      goto remove_error;

    gst_object_unref (node);
  }

  return TRUE;

no_listener:
  {
    GST_WARNING ("Listener is not in the connected listeners list");
    return FALSE;
  }
no_node:
  {
    GST_WARNING ("Node %s not found. Could not leave node.",
        listener_priv->listen_to);
    g_free (listener_priv->listen_to);
    listener_priv->listen_to = NULL;
    return FALSE;
  }
remove_error:
  {
    GST_WARNING
        ("The listener %s was not listening to %s, there's something very wrong",
        listener_name, listener_priv->listen_to);
    gst_object_unref (node);
    g_free (listener_priv->listen_to);
    listener_priv->listen_to = NULL;
    return FALSE;
  }

}

gboolean
gst_inter_pipe_leave_node (GstInterPipeIListener * listener)
{
  gboolean ret = TRUE;

  g_rec_mutex_lock (&listeners_mutex);

  ret = gst_inter_pipe_leave_node_priv (listener);
  if (!ret)
    goto out;

  if (!gst_inter_pipe_leave_listeners_table (listener))
    goto list_error;

out:
  g_rec_mutex_unlock (&listeners_mutex);
  return ret;

list_error:
  {
    GST_WARNING ("Could not leave node");
    g_rec_mutex_unlock (&listeners_mutex);
    return FALSE;
  }
}

static void
gst_inter_pipe_notify_node_added (gpointer _listener_key, gpointer _listener,
    gpointer data)
{
  GstInterPipeListenerPriv *listener_priv = _listener;
  GstInterPipeIListener *listener = listener_priv->listener;
  gchar *node_name = data;

  GST_INFO ("Notifying new node added: %s", node_name);

  gst_inter_pipe_ilistener_node_added (listener, node_name);
}

gboolean
gst_inter_pipe_add_node (GstInterPipeINode * node, const gchar * node_name)
{
  GHashTable *nodes;
  GHashTable *listeners;

  g_return_val_if_fail (node != NULL, FALSE);
  g_return_val_if_fail (node_name != NULL, FALSE);

  g_mutex_lock (&nodes_mutex);

  nodes = gst_inter_pipe_get_nodes ();
  if (g_hash_table_contains (nodes, node_name))
    goto no_unique;

  GST_INFO ("Adding node %s", node_name);

  {
    /* The table stores a GWeakRef box, so it still does not keep the node
     * alive, but a concurrent gst_inter_pipe_get_node can no longer resurrect
     * it mid-finalize. The g_hash_table_contains check above runs under this
     * same lock and guarantees the key is unique, so the insert always adds a
     * fresh entry and the table takes ownership of the box through its
     * value-destroy func. */
    GWeakRef *weak = g_new0 (GWeakRef, 1);

    g_weak_ref_init (weak, node);
    g_hash_table_insert (nodes, g_strdup (node_name), (gpointer) weak);
  }

  g_mutex_unlock (&nodes_mutex);

  g_rec_mutex_lock (&listeners_mutex);
  listeners = gst_inter_pipe_get_listeners ();
  g_hash_table_foreach (listeners, gst_inter_pipe_notify_node_added,
      (gpointer) node_name);
  g_rec_mutex_unlock (&listeners_mutex);

  return TRUE;

no_unique:
  {
    GST_WARNING ("Could not add node %s, it is not unique.", node_name);
    g_mutex_unlock (&nodes_mutex);
    return FALSE;
  }
}

gboolean
gst_inter_pipe_remove_node (GstInterPipeINode * node, const gchar * node_name)
{
  GHashTable *nodes;
  GHashTable *listeners;
  GList *listener_list;
  GList *l;

  g_return_val_if_fail (node != NULL, FALSE);
  g_return_val_if_fail (node_name != NULL, FALSE);

  g_mutex_lock (&nodes_mutex);

  nodes = gst_inter_pipe_get_nodes ();
  GST_INFO ("Removing node %s", node_name);
  if (!g_hash_table_remove (nodes, (gconstpointer) node_name)) {
    GST_WARNING ("Node %s not found. Could not remove it.", node_name);
    g_mutex_unlock (&nodes_mutex);
    return FALSE;
  }
  g_mutex_unlock (&nodes_mutex);

  g_rec_mutex_lock (&listeners_mutex);
  listeners = gst_inter_pipe_get_listeners ();

  /* Snapshot the listeners, holding a ref on each: a listener that is attached
   * to this node leaves it from its node_removed handler, which removes its
   * entry from this very table, and g_hash_table_foreach may not run over a
   * table that the callback modifies. The ref keeps the listener alive across
   * the notification even though leaving drops the table's own ref. */
  listener_list = g_hash_table_get_values (listeners);
  for (l = listener_list; l != NULL; l = l->next) {
    GstInterPipeListenerPriv *listener_priv = l->data;

    l->data = gst_object_ref (listener_priv->listener);
  }

  for (l = listener_list; l != NULL; l = l->next) {
    GST_INFO ("Notifying node removed: %s", node_name);
    gst_inter_pipe_ilistener_node_removed (l->data, node_name);
  }

  g_list_free_full (listener_list, gst_object_unref);
  g_rec_mutex_unlock (&listeners_mutex);

  return TRUE;
}
