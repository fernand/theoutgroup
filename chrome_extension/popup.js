function getCurrentTabUrl(callback) {
  chrome.tabs.query({active: true, currentWindow: true}, function(tabs) {
    callback(tabs[0].url);
  });
}

function post(data, url) {
  return new Promise(function(resolve, reject) {
    var request = new XMLHttpRequest();
    request.open('POST', url, true);
    request.setRequestHeader('Content-type', 'application/json');
    request.onload = function() {
      if (request.status >= 200 && request.status < 400) {
        var json = JSON.parse(request.response);
        return resolve(json);
      } else {
        return resolve([]);
      }
    }
    request.send(JSON.stringify(data));
  });
}

function getHostName(url) {
  var hostName = '';
  if (url.indexOf('://') > -1) {
    hostName = url.split('/')[2];
  } else {
    hostName = url.split('/')[0];
  }
  return hostName.split(':')[0];
}

function appendArticle(articleData) {
  var articleHTML = '<a href="http://link.com" target="_blank" class="link"><div class="spectrum"></div><div class="title"></div><div class="outlet"><img src="http://www.google.com/s2/favicons?domain=" class="outlet_image"><div></div></div></a>';
  var parent = document.querySelector('.articles');
  var article = document.createElement('div');
  article.className = 'article';
  article.innerHTML = articleHTML;
  var a = article.childNodes[0];
  var spectrum = a.childNodes[0];
  var title = a.childNodes[1];
  var outlet = a.childNodes[2];
  a.href = articleData.url;
  spectrum.textContent = 'Spectrum ' + articleData.scalar;
  title.textContent = articleData.title;
  var img = outlet.childNodes[0];
  var hostName = getHostName(articleData.url);
  img.src += hostName;
  outlet.childNodes[1].textContent = hostName;
  parent.appendChild(article);
}

document.addEventListener('DOMContentLoaded', function() {
  getCurrentTabUrl(function(url) {
    post({'url': url}, 'http://api.theoutgroup.org:5000/similar')
    .then((articles) => {
      var parent = document.querySelector('.articles');
      while (parent.firstChild) {
        parent.removeChild(parent.firstChild);
      }
      articles.forEach(function(article) {appendArticle(article)})
    })
    .catch((err) => {console.log(err)});
  });
});
