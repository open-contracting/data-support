import type { Exports } from "ids-drr-branding-types";

import heroBackground from "./assets/heroBackground.jpg";
import heroForeground from "./assets/heroForeground.svg";
import logo from "./assets/logo.svg";
import paraguayIcon from "./assets/paraguay-icon.svg";
import { Footer } from "./components/footer";
import { IntroSection } from "./components/intro-section";
import messagesEs from "./messages/es.json";
import messagesGn from "./messages/gn.json";

export { Footer, IntroSection };
export const AboutPage: Exports["AboutPage"] = undefined;
export const OutroSection: Exports["OutroSection"] = undefined;
export const Credits: Exports["Credits"] = undefined;
export const PartnerLogos: Exports["PartnerLogos"] = undefined;

export const config: Exports["config"] = {
  logo,
  heroBackground: heroBackground.src,
  heroForeground,
  states: [
    {
      name: "Paraguay",
      slug: "paraguay",
      icon: paraguayIcon,
      status: "active",
      zoom: 5.5,
      minZoom: 5,
    },
  ],
  locales: ["es", "gn"],
  defaultLocale: "es",
  messages: {
    es: messagesEs,
    gn: messagesGn,
  },
};
