import React from "react";
import './Banner.css'
import warnImg from "./bootstrap-icons/icons/exclamation-circle.svg";
import {Link} from "react-router-dom";

export function ShowBanner(msg, isError) {
    const banner = document.getElementById('banner')
    banner.classList.add('showing')
    if (isError) {
        banner.classList.add('error')
    }
    setTimeout(function() {
        banner.classList.remove('showing')
    }, 5000)

    document.querySelector('#banner .message').textContent = msg
}

export function Banner(props) {
    return (
        <div id="banner">
            <div className="message"></div>
        </div>
    )
}
