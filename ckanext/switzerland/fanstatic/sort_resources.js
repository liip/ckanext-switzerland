/* If this looks like a really ugly hack to you, you are absolutely right.
 * Our harvesters sort the resources inside datasets after adding/deleting
 * them. But somehow after some time the order breaks (at least on the istdaten
 * dataset), probably because the ckan datapusher does something silly.
 * This hack sorts the resources on the client side.
 */
$(function() {
  if (window.location.href.match(/\/dataset\/istdaten/)) {
    var list = $('#dataset-resources .resource-list')
    var items = list.find('.resource-item');
    if (items.length) {
      items.detach().sort(function (a, b) {
        var heading1 = $(a).find('.heading').attr('title');
        var heading2 = $(b).find('.heading').attr('title');
        return -heading1.localeCompare(heading2);
      });
      list.append(items);
    }
  }
});
