import { Component, OnInit, Inject } from '@angular/core';
import { SharedService } from '../../services/shared.service';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { VideoPlayerData } from '../../constants/video-player-type';

@Component({
  selector: 'app-video-player',
  templateUrl: './video-player.component.html',
  styleUrls: ['./video-player.component.scss']
})
export class VideoPlayerComponent implements OnInit {

  constructor(private sharedService: SharedService,
    public dialogRef: MatDialogRef<VideoPlayerComponent>,
    @Inject(MAT_DIALOG_DATA) public data: VideoPlayerData) { }

  ngOnInit() {
  }

}
