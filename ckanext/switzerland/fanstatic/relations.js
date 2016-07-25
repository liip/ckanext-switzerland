"use strict";

ckan.module('switzerland_relations', function ($, _) {
  return {
    initialize: function () {
      var relations_el = this.el;
      var module = this;

      module._init_remove_buttons();

      // insert new form when add button is clicked
      relations_el.find('.add-button').click(function() {
        var last_form = relations_el.find('.relations-form:last');
        var new_form = last_form.clone(true);

        // replace id, name and for attributes on cloned form
        var last_index = parseInt(last_form.data('id'));
        new_form.find('input,label').each(function() {
          var el = $(this);
          $.each(['id', 'name', 'for'], function(index, attr_name) {
            var attr_value = el.attr(attr_name);
            if (attr_value) {
              el.attr(attr_name, attr_value.replace(last_index, last_index + 1))
            }
          });
        });
        new_form.data('id', last_index + 1);
        new_form.find('input').val('');
        new_form.insertAfter(last_form);

        // potentially show remove buttons on all forms when there was only one form before
        module._init_remove_buttons();
        return false;
      });

      // remove form on remove button press, potentially hiding the remove button when only one form is left
      relations_el.find('.remove-button').click(function() {
        $(this).parents('.relations-form').remove();
        module._init_remove_buttons();
      });
    },

    _init_remove_buttons: function() {
      var remove_buttons = this.el.find('.remove-button');
      if (remove_buttons.length < 2) {
        remove_buttons.hide();
      } else {
        remove_buttons.show();
      }
    }
  };
});
