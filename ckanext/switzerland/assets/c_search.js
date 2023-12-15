Vue.config.delimiters = ['[[', ']]'];
Vue.config.unsafeDelimiters = ['[[[', ']]]'];

new Vue({
  el: '#app',
  data: {
    currentSearchTerm: '',
    searchTerm: '',
    pageResults: [],
    datasetResults: [],
    faqResults: [],

    language: '',
    // shows an error in gui when true
    hasError: false,
    loading: false,
    paginatePages: {
      currentPage: 1,
      itemsPerPage: 8,
      resultCount: 0
    },
    paginateDatasets: {
      currentPage: 1,
      itemsPerPage: 5,
      resultCount: 0
    },
    paginateFAQs: {
      currentPage: 1,
      itemsPerPage: 5,
      resultCount: 0
    }
  },
  computed: {
      totalPages: function() {
        return Math.ceil(this.paginatePages.resultCount / this.paginatePages.itemsPerPage)
      },
      totalDatasets: function() {
        return Math.ceil(this.paginateDatasets.resultCount / this.paginateDatasets.itemsPerPage)
      },
      totalFAQs: function() {
        return Math.ceil(this.paginateFAQs.resultCount / this.paginateFAQs.itemsPerPage)
      }
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
          self.searchTerm = decodeURIComponent(pair[1].replace(/\+/g, '%20'))
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

      this.hasError = false
      self.currentSearchTerm = self.searchTerm
      this.loading = true

      var ckanSearch = $.get('/api/3/action/package_search', {
        'facet.limit': 100,
        'q': this.searchTerm,
        'rows': 1000,
      });
      var pageSearch = $.get('/wp-json/wp/v2/pages/', {
        'per_page': 100,
        'search': this.searchTerm,
        'lang': self.language
      });
      var faqSearch = $.get('/wp-json/wp/v2/hrf_faq/', {
        'per_page': 100,
        'search': this.searchTerm,
        'lang': self.language
      });
      $.when(ckanSearch, pageSearch, faqSearch).then(function(datasets, pages, faqs) {
        // ckan search results
        self.datasetResults = []
        datasets[0].result.results.map(function(result) {
          let description = {de: '', en: '', fr: '', it: ''};
          result.extras.foreach((item, index) => {
            if (item.key === 'description') {
              description = item.value
            }
          })
          self.datasetResults.push({
            title: result.title[self.language],
            description: description[self.language],
            link: '/' + self.language + '/dataset/' + result.name
          })
        })

        // wordpress pages search results
        self.pageResults = []
        pages[0].map(function(result) {
          self.pageResults.push({
            title: result.title.rendered,
            description: result.excerpt.rendered,
            link: result.link
          })
        })

        // wordpress faq search results
        self.faqResults = []
        faqs[0].map(function(result) {
          self.faqResults.push({
            title: result.title.rendered,
            description: result.content.rendered,
            link: result.link
          })
        })

        self.loading = false
      },
      /**
       * Error handler
       */
      function() {
      	self.hasError = true
      	self.loading = false
      	self.pageResults = []
      	self.datasetResults = []
      })
    },
    setPage: function(pageNumber, paginate) {
      paginate.currentPage = pageNumber
    }
  },
  filters: {
    paginate: function(list, paginate) {
      paginate.resultCount = list.length;
      if (paginate == this.paginatePages && paginate.currentPage >= this.totalPages) {
        paginate.currentPage = this.totalPages
      }
      else if (paginate == this.paginateDatasets && paginate.currentPage >= this.totalDatasets) {
        paginate.currentPage = this.totalDatasets
      }
      else if (paginate == this.paginateFAQs && paginate.currentPage >= this.totalFAQs) {
        paginate.currentPage = this.totalFAQs
      }
      var index = paginate.currentPage * paginate.itemsPerPage
      return list.slice(index, index + paginate.itemsPerPage)
    }
  }
})
