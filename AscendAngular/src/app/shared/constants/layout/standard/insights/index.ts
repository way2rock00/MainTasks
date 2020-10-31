import { ESTABLISH_LAYOUT } from './establish-layout.config';
import { DEVELOP_LAYTOUT } from './develop-layout.config';
import { ESTIMATE_LAYTOUT } from './estimate-layout.config';
import { DISCOVER_LAYTOUT } from './discover-layout.config';
import { CREATE_LAYTOUT } from './create-layout.config';
import { LAYOUT_INSIGHTS_SUB_NAV } from '../standard-constants.model';
import { SENSE_LAYOUT } from './sense-layout.config';
import { DEFINE_DIGITAL_LAYOUT} from './define-digital-layout.config';
import { DEFINE_JOURNEY_LAYOUT } from './define-journey-layout.config';

export default {
    [LAYOUT_INSIGHTS_SUB_NAV.SENSE]    : SENSE_LAYOUT,
    [LAYOUT_INSIGHTS_SUB_NAV.DEFINE_DIGITAL]    : DEFINE_DIGITAL_LAYOUT,
    [LAYOUT_INSIGHTS_SUB_NAV.DEFINE_JOURNEY]    : DEFINE_JOURNEY_LAYOUT,
    [LAYOUT_INSIGHTS_SUB_NAV.CREATE]    : CREATE_LAYTOUT,
    [LAYOUT_INSIGHTS_SUB_NAV.DISCOVER]  : DISCOVER_LAYTOUT,
    [LAYOUT_INSIGHTS_SUB_NAV.ESTIMATE]  : ESTIMATE_LAYTOUT,
    [LAYOUT_INSIGHTS_SUB_NAV.DEVELOP]   : DEVELOP_LAYTOUT,
    [LAYOUT_INSIGHTS_SUB_NAV.ESTABLISH] : ESTABLISH_LAYOUT,

}
