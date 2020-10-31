import { STABILIZE_LAYTOUT } from './stabilize-layout.config';
import { CONTINUE_LAYTOUT } from './continue-layout.config';
import { OPTIMIZE_LAYTOUT } from './optimize-layout.config';
import { LAYOUT_RUN_SUB_NAV } from './../standard-constants.model';

export default {
    [LAYOUT_RUN_SUB_NAV.OPTIMIZE]  : OPTIMIZE_LAYTOUT,
    [LAYOUT_RUN_SUB_NAV.CONTINUE]  : CONTINUE_LAYTOUT,
    [LAYOUT_RUN_SUB_NAV.STABILIZE] : STABILIZE_LAYTOUT
}
