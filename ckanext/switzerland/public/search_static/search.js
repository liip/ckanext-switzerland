
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
    var lang = document.getElementsByTagName('html')[0].getAttribute('lang');
    this.language = lang ? lang.split('-')[0] : 'en'
  },
  methods: {
    search: function() {
      var self = this;
      self.currentSearchTerm = self.searchTerm;
      $.ajax('/api/3/action/package_search?q=' + this.searchTerm).done(function(data) {
        self.datasetResults = []
        data.result.results.map(function(result) {
          console.log(result)
          self.datasetResults.push({
            title: result.title[self.language],
            description: result.description[self.language],
            link: '/' + self.language + '/dataset/' + result.name
          })
        })
      })
    }
  }
})
