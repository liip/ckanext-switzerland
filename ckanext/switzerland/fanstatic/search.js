
new Vue({
  el: '#app',
  data: {
    currentSearchTerm: '',
    searchTerm: '',
    pageResults: [],
    datasetResults: [],
    language: ''
  },
  ready: function() {
    var self = this
    var lang = $('html').attr('lang')
    this.language = lang ? lang.split('-')[0] : 'en'

    var queryString = window.location.search
    if (queryString) {
      $.each(queryString.substring(1).split('&'), function(index, param) {
        var pair = param.split('=')
        if (pair[0] == 'q') {
          self.searchTerm = pair[1]
          if (self.searchTerm.length >= 3) {
            self.search()
          }
        }
      })
    }
  },
  methods: {
    search: function() {
      var self = this

      var ckanSearch = $.ajax('/api/3/action/package_search?q=' + this.searchTerm)
      var wordPressSearch = $.ajax('/wp-json/wp/v2/search/' + this.searchTerm)

      $.when(ckanSearch, wordPressSearch).done(function(datasets, pages) {
        // ckan search results
        self.datasetResults = []
        datasets[0].result.results.map(function(result) {
          self.datasetResults.push({
            title: result.title[self.language],
            description: result.description[self.language],
            link: '/' + self.language + '/dataset/' + result.name
          })
        })

        // wordpress search results
        self.pageResults = []
        pages[0].map(function(result) {
          self.pageResults.push({
            title: result.title.rendered,
            description: result.excerpt.rendered,
            link: result.link
          })
        })

        self.currentSearchTerm = self.searchTerm
      })
    }
  }
})
