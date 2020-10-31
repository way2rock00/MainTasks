interface CarouselCardI {
    header: string;
    subheader: string;
    description: string;
    themeKey: string;
    imageName: string;
   

}


/* -- Data for rendering Insight card on home page -- */

const HOME_INSIGHTS_CONSTANT: CarouselCardI =
{
    header: "Insights",
    subheader: "Ambitions | Value | Exploring digital",
    description: "Explore digital platform, ambitions and value it provides",
    themeKey: "insights",
    imageName: "Insights - wheel.svg" 
    
}

/* -- Data for rendering Imagine card on home page -- */

const HOME_IMAGINE_CONSTANT: CarouselCardI =
{
    header: "Imagine",
    subheader: "Digital native | Personalized | Becoming digital",
    description: "Decide your personalized digital strategy",
    themeKey: "imagine",
    imageName: "Imagine - wheel.svg"
}

/* --Data for rendering Deliver card on home page -- */
const HOME_DELIVER_CONSTANT: CarouselCardI =
{
    header: "Deliver",
    subheader: "Iterative | Digital capabilities | Doing digital",
    description: "Build your capabilities and start your implementation",
    themeKey: "deliver",
    imageName: "Deliver_all_OFF.svg"
  
   
}
/* -- Data for rendering Deliver card on home page -- */

const HOME_RUN_CONSTANT: CarouselCardI =
{
    header: "Run",
    subheader: "Digital foundry | Efficient | Being digital",
    description: "Become fully digital by implementing efficient and sustainable solutions",
    themeKey: "run",
    imageName: "Run - wheel.svg"
    
}


export const HOME_CONSTANT: CarouselCardI[] = [HOME_INSIGHTS_CONSTANT, HOME_IMAGINE_CONSTANT, HOME_DELIVER_CONSTANT, HOME_RUN_CONSTANT]







