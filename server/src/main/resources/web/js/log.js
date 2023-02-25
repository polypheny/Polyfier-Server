/*
 * Copyright 2019-2023 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const log = document.getElementById('log-area');

log.textContent = ""

function httpGet(url) {
    let xmlHttp = new XMLHttpRequest();
    xmlHttp.open("GET", url, false)
    xmlHttp.send( null )
    return xmlHttp;
}

const x = setInterval(function () {
    let target = location.protocol + "//" + location.host + "/request/log.json"
    let response = httpGet(target)
    console.log(response)
    if ( response.statusText !== "No Content" ) {
        log.textContent += response.responseText
    }
}, 5000);


var countDownDate = new Date("February 28, 2023 00:00:00").getTime();

const y = setInterval(function() {
    var now = new Date().getTime();
    var distance = countDownDate - now;

    var days = Math.floor(distance / (1000 * 60 * 60 * 24));
    var hours = Math.floor((distance % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
    var minutes = Math.floor((distance % (1000 * 60 * 60)) / (1000 * 60));
    var seconds = Math.floor((distance % (1000 * 60)) / 1000);

    document.getElementById("countdown").innerHTML = days + "d " + hours + "h "
        + minutes + "m " + seconds + "s ";

    if (distance < 0) {
        clearInterval(x);
        document.getElementById("countdown").innerHTML = "EXPIRED";
    }
}, 1000);


const z = setInterval(function () {
    let target = location.protocol + "//" + location.host + "/request/status-update.json"
    let response = httpGet(target)
    let jsonResponse = JSON.parse(response.responseText)
    document.getElementById("server-status").innerHTML = jsonResponse["server-status"]
    document.getElementById("server-status").style.color = (jsonResponse["server-status"] === "RUNNING") ? "green":"red";
    document.getElementById("database-status").innerHTML = jsonResponse["database-status"]
    document.getElementById("database-status").style.color = (jsonResponse["database-status"] === "CONNECTED") ? "green":"red";
    document.getElementById("docker-status").innerHTML = jsonResponse["docker-status"]
    document.getElementById("docker-status").style.color = (jsonResponse["docker-status"] === "CONNECTED") ? "green":"red";
    document.getElementById("polyfier-phase").innerHTML = jsonResponse["polyfier-phase"]
    document.getElementById("polypheny-control-status").innerHTML = jsonResponse["polypheny-control"]
    document.getElementById("polypheny-control-status").style.color = (jsonResponse["polypheny-control"] === "CONNECTED") ? "green":"red";
    document.getElementById("pdb-client-status").innerHTML = jsonResponse["polypheny-db"]
    document.getElementById("pdb-client-status").style.color = (jsonResponse["polypheny-db"] === "CONNECTED") ? "green":"red";
    document.getElementById("defcon").innerHTML = jsonResponse["defcon"]
    if ( jsonResponse["defcon"] === "5" ) {
        document.getElementById("defcon").style.color = "green";
    } else if ( jsonResponse["defcon"] === "4" || jsonResponse["defcon"] === "3" ) {
        document.getElementById("defcon").style.color = "orange";
    } else if ( jsonResponse["defcon"] === "2" || jsonResponse["defcon"] === "1") {
        document.getElementById("defcon").style.color = "red";
    }
}, 10000);