/* Module for reordering resources
 */
this.ckan.module('bs3-resource-reorder', function($, _) {
  return {
    options: {
      id: false,
      i18n: {
        label: _('Reorder resources'),
        save: _('Save order'),
        saving: _('Saving...'),
        cancel: _('Cancel')
      }
    },
    template: {
      title: '<h1></h1>',
      button: [
        '<a href="javascript:;" class="btn btn-default">',
        '<i class="icon-reorder"></i>',
        '<span></span>',
        '</a>'
      ].join('\n'),
      form_actions: [
        '<div class="form-actions">',
        '<a href="javascript:;" class="cancel btn btn-default pull-left"></a>',
        '<a href="javascript:;" class="save btn btn-primary"></a>',
        '</div>'
      ].join('\n'),
      handle: [
        '<a href="javascript:;" class="handle">',
        '<i class="icon-move"></i>',
        '</a>'
      ].join('\n'),
      saving: [
        '<span class="saving muted m-right">',
        '<i class="icon-spinner icon-spin"></i>',
        '<span></span>',
        '</span>'
      ].join('\n')
    },
    is_reordering: false,
    cache: false,

    initialize: function() {
      jQuery.proxyAll(this, /_on/);

      this.html_title = $(this.template.title)
        .text(this.i18n('label'))
        .insertBefore(this.el)
        .hide();
      var button = $(this.template.button)
        .on('click', this._onHandleStartReorder)
        .appendTo('.page_primary_action');
      $('span', button).text(this.i18n('label'));

      this.html_form_actions = $(this.template.form_actions)
        .hide()
        .insertAfter(this.el);
      $('.save', this.html_form_actions)
        .text(this.i18n('save'))
        .on('click', this._onHandleSave);
      $('.cancel', this.html_form_actions)
        .text(this.i18n('cancel'))
        .on('click', this._onHandleCancel);

      this.html_handles = $(this.template.handle)
        .hide()
        .appendTo($('.resource-item', this.el));

      this.html_saving = $(this.template.saving)
        .hide()
        .insertBefore($('.save', this.html_form_actions));
      $('span', this.html_saving).text(this.i18n('saving'));

      this.cache = this.el.html();

      this.el
        .sortable()
        .sortable('disable');

    },

    _onHandleStartReorder: function() {
      if (!this.is_reordering) {
        this.html_form_actions
          .add(this.html_handles)
          .add(this.html_title)
          .show();
        this.el
          .addClass('reordering')
          .sortable('enable');
        $('.page_primary_action').hide();
        this.is_reordering = true;
      }
    },

    _onHandleCancel: function() {
      if (
        this.is_reordering
        && !$('.cancel', this.html_form_actions).hasClass('disabled')
      ) {
        this.reset();
        this.is_reordering = false;
        this.el.html(this.cache)
          .sortable()
          .sortable('disable');
        this.html_handles = $('.handle', this.el);
      }
    },

    _onHandleSave: function() {
      if (!$('.save', this.html_form_actions).hasClass('disabled')) {
        var module = this;
        module.html_saving.show();
        $('.save, .cancel', module.html_form_actions).addClass('disabled');
        var order = [];
        $('.resource-item', module.el).each(function() {
          order.push($(this).data('id'));
        });
        module.sandbox.client.call('POST', 'package_resource_reorder', {
          id: module.options.id,
          order: order
        }, function() {
          module.html_saving.hide();
          $('.save, .cancel', module.html_form_actions).removeClass('disabled');
          module.cache = module.el.html();
          module.reset();
          module.is_reordering = false;
        });
      }
    },

    reset: function() {
      this.html_form_actions
        .add(this.html_handles)
        .add(this.html_title)
        .hide();
      this.el
        .removeClass('reordering')
        .sortable('disable');
      $('.page_primary_action').show();
    }

  };
});
