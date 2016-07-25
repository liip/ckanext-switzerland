"use strict";

ckan.module('switzerland_relations', function ($, _) {
  return {
    initialize: function () {
      var relations_el = this.el;

      relations_el.find('.add-button').click(function() {
        var last_form = relations_el.find('.relations-form:last');
        var new_form = last_form.clone();

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
        new_form.insertAfter(last_form);
        return false;
      });
    }
  };
});
