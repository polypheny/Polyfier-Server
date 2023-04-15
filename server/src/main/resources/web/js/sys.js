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




document.addEventListener("DOMContentLoaded", function() {
    const socketUrl = "ws://" + window.location.host + "/ws";
    const socket = new WebSocket(socketUrl);

    socket.addEventListener("open", (event) => {
        console.log("WebSocket connected:", socket);

        interval = setInterval(function() {
            sendMessage(
                JSON.stringify({
                    clientCode:"BROWSER",
                    messageCode:"BROWSER_SYS",
                })
            )
        }, 5000 );

    });

    socket.addEventListener("message", (event) => {
        console.log("Received message:", event.data.toString());
        // Process the message and optionally send a response back to the server.
        var content = JSON.parse(event.data);
        if (content.log) {
            console.log("Log message:", content.log);
        } else if (content.sys) {
            console.log("System message:", content.sys);
            // Todo display info -> D3
        } else {
            console.log("Unknown content:", content);
        }
        console.log(content.key)
    });

    socket.addEventListener("close", (event) => {
        console.log("WebSocket closed:", event);
    });

    socket.addEventListener("error", (event) => {
        console.error("WebSocket error:", event);
    });

    // Example: sending a message to the server
    function sendMessage(message) {
        socket.send(message);
    }

});