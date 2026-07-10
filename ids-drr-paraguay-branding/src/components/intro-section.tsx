"use client";

import Image from "next/image";
import { Text } from "opub-ui";

import heroBackground from "../assets/heroBackground.jpg";
import heroForeground from "../assets/heroForeground.svg";
import { useDynamicTranslations } from "../hooks/use-dynamic-translations";
import styles from "./intro-section.module.scss";

// Paraguay-specific hero. Mirrors the default frontend HeroSection layout
// but adds a dark bottom-to-transparent gradient scrim so the tagline
// stays legible against the photo's lighter regions.
export function IntroSection() {
  const t = useDynamicTranslations("home.hero");
  const tSite = useDynamicTranslations("site");

  return (
    <section
      className={styles.heroSection}
      style={{ backgroundImage: `url(${heroBackground.src})` }}
      aria-labelledby="hero-heading"
    >
      <div aria-hidden="true" className={styles.scrim} />
      <div className="container relative z-10 flex h-full w-full flex-col items-center justify-end self-center py-14">
        <Text id="hero-heading" className="sr-only" variant="heading4xl" as="h1">
          {tSite("name")}
        </Text>
        <Image
          src={heroForeground}
          alt=""
          sizes={`(min-width: 1024px) ${heroForeground.width}px, 100vw`}
          className="block h-auto max-w-full lg:max-w-none"
        />
        <Text className="pt-4 text-center text-surfaceDefault" fontWeight="regular" variant="headingMd">
          {t("tagline")}
        </Text>
      </div>
    </section>
  );
}
