import React from "react";
import './Banner.css'

export function ShowBanner(msg) {
    const banner = document.getElementById('banner')
    banner.classList.add('showing')
    setTimeout(function() {
        banner.classList.remove('showing')
    }, 3000)

    document.querySelector('#banner .message').textContent = msg
}

export function Banner(props) {
    return (
        <div id="banner">
            <div className="message"></div>
        </div>
    )
}
